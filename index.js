require('dotenv').config();  
  
const path = require('path');  
const zlib = require('zlib');  
const crypto = require('crypto');  
const fs = require('fs');  
const http = require('http');  
const url = require('url');  
  
const axios = require('axios');  
const pino = require('pino');  
const QRCode = require('qrcode');  
  
const {  
  default: makeWASocket,  
  DisconnectReason,  
  useMultiFileAuthState,  
  Browsers,  
  fetchLatestBaileysVersion,  
  downloadMediaMessage,  
  makeInMemoryStore  
} = require('@whiskeysockets/baileys');  
  
require('dotenv').config();  
  
// ─────────────────────────────────────────// UPSTASH REDIS// ─────────────────────────────────────────  
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;  
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;  
  
const upstashEnabled = !!(REDIS_URL && REDIS_TOKEN);  
  
async function redisGet(key) {  
  try {  
    const r = await axios.get(`${REDIS_URL}/get/${encodeURIComponent(key)}`, {  
      headers: { Authorization: `Bearer ${REDIS_TOKEN}` },  
      timeout: 8000  
    });  
    if (!r?.data) return null;  
    if (r.data.result === null) return null;  
    return JSON.parse(r.data.result);  
  } catch {  
    return null;  
  }  
}  
  
async function redisSet(key, value) {  
  try {  
    await axios.post(  
      `${REDIS_URL}/set/${encodeURIComponent(key)}`,  
      { value: JSON.stringify(value) },  
      {  
        headers: {  
          Authorization: `Bearer ${REDIS_TOKEN}`,  
          'Content-Type': 'application/json'  
        },  
        timeout: 8000  
      }  
    );  
    return true;  
  } catch {  
    return false;  
  }  
}  
  
async function redisDel(key) {  
  try {  
    await axios.post(  
      `${REDIS_URL}/del/${encodeURIComponent(key)}`,  
      {},  
      {  
        headers: {  
          Authorization: `Bearer ${REDIS_TOKEN}`  
        },  
        timeout: 8000  
      }  
    );  
    return true;  
  } catch {  
    return false;  
  }  
}  
  
// ─────────────────────────────────────────// WHATSAPP AUTH PERSISTENCE (Upstash) // ─────────────────────────────────────────  
const WA_AUTH_DIR = process.env.WHATSAPP_AUTH_DIR || '/tmp/auth_info';  
  
// Use prefix so multiple bots don't collide  
const WA_AUTH_PREFIX =  
  process.env.WHATSAPP_UPSTASH_AUTH_PREFIX || 'mega-agency-bot:wa-auth:v2';  
  
const WA_AUTH_CREDS_KEY = `${WA_AUTH_PREFIX}:creds`;  
const WA_AUTH_SNAPSHOT_KEY = `${WA_AUTH_PREFIX}:snapshot`;  
  
// Snapshot can be large; by default save only creds (enough to prevent re-login in most hosts).  
const WA_AUTH_SAVE_SNAPSHOT =  
  (process.env.WHATSAPP_UPSTASH_SAVE_SNAPSHOT || 'false').toLowerCase() === 'true';  
  
const WA_AUTH_SNAPSHOT_MAX_BYTES = parseInt(  
  process.env.WHATSAPP_UPSTASH_SNAPSHOT_MAX_BYTES || '2500000',  
  10  
);  
  
const WA_AUTH_PERSIST_THROTTLE_MS = parseInt(  
  process.env.WHATSAPP_UPSTASH_PERSIST_THROTTLE_MS || '8000',  
  10  
);  
  
let lastAuthPersistAt = 0;  
let authPersistInFlight = false;  
let authPersistPending = false;  
  
function ensureDirSync(dir) {  
  fs.mkdirSync(dir, { recursive: true });  
}  
  
async function restoreWhatsAppAuthFromUpstash() {  
  if (!upstashEnabled) return;  
  
  ensureDirSync(WA_AUTH_DIR);  
  
  const credsPath = path.join(WA_AUTH_DIR, 'creds.json');  
  const localCredsExists = fs.existsSync(credsPath);  
  
  const forceRestore =  
    (process.env.WHATSAPP_AUTH_FORCE_RESTORE || 'false').toLowerCase() === 'true';  
  
  if (localCredsExists && !forceRestore) return;  
  
  // Try snapshot first (if enabled previously)  
  try {  
    const snap = await redisGet(WA_AUTH_SNAPSHOT_KEY);  
    if (snap?.data) {  
      const gz = Buffer.from(snap.data, 'base64');  
      const jsonBuf = zlib.gunzipSync(gz);  
      const parsed = JSON.parse(jsonBuf.toString('utf8'));  
  
      if (parsed?.files && typeof parsed.files === 'object') {  
        fs.rmSync(WA_AUTH_DIR, { recursive: true, force: true });  
        fs.mkdirSync(WA_AUTH_DIR, { recursive: true });  
  
        for (const [relPath, contentB64] of Object.entries(parsed.files)) {  
          const fullPath = path.join(WA_AUTH_DIR, relPath);  
          ensureDirSync(path.dirname(fullPath));  
          fs.writeFileSync(fullPath, Buffer.from(contentB64, 'base64'));  
        }  
        console.log('✅ WhatsApp auth restored from Upstash snapshot!');  
        return;  
      }  
    }  
  } catch (e) {  
    console.log('Auth snapshot restore failed:', e.message);  
  }  
  
  // Restore creds.json only  
  try {  
    const creds = await redisGet(WA_AUTH_CREDS_KEY);  
    if (creds) {  
      fs.writeFileSync(credsPath, JSON.stringify(creds, null, 2));  
      console.log('✅ WhatsApp creds restored from Upstash!');  
    }  
  } catch {  
    // ignore  
  }  
}  
  
function collectAuthDirFilesSnapshot(authDir) {  
  const files = {};  
  const stack = [authDir];  
  
  while (stack.length) {  
    const current = stack.pop();  
    const entries = fs.readdirSync(current, { withFileTypes: true });  
  
    for (const ent of entries) {  
      const full = path.join(current, ent.name);  
      if (ent.isDirectory()) {  
        stack.push(full);  
        continue;  
      }  
      if (!ent.isFile()) continue;  
  
      const rel = path.relative(authDir, full).replace(/\\/g, '/');  
      const buf = fs.readFileSync(full);  
      files[rel] = buf.toString('base64');  
    }  
  }  
  
  return { files };  
}  
  
async function persistWhatsAppAuthToUpstashThrottled(force = false) {  
  if (!upstashEnabled) return;  
  
  if (authPersistInFlight) {  
    if (force) authPersistPending = true;  
    return;  
  }  
  
  const now = Date.now();  
  if (!force && now - lastAuthPersistAt < WA_AUTH_PERSIST_THROTTLE_MS) return;  
  
  authPersistInFlight = true;  
  authPersistPending = false;  
  
  try {  
    const credsPath = path.join(WA_AUTH_DIR, 'creds.json');  
    if (!fs.existsSync(credsPath)) return;  
  
    // Always persist creds  
    const creds = JSON.parse(fs.readFileSync(credsPath, 'utf8'));  
    await redisSet(WA_AUTH_CREDS_KEY, creds);  
  
    // Optional: persist full snapshot  
    if (WA_AUTH_SAVE_SNAPSHOT) {  
      try {  
        const snapshot = collectAuthDirFilesSnapshot(WA_AUTH_DIR);  
        const snapshotObj = { v: 1, files: snapshot.files };  
        const jsonStr = JSON.stringify(snapshotObj);  
  
        const gz = zlib.gzipSync(Buffer.from(jsonStr, 'utf8'), { level: 9 });  
  
        if (gz.length <= WA_AUTH_SNAPSHOT_MAX_BYTES) {  
          await redisSet(WA_AUTH_SNAPSHOT_KEY, {  
            v: 1,  
            data: gz.toString('base64'),  
            t: Date.now()  
          });  
        } else {  
          // Too large - skip snapshot to avoid errors.  
          // Keep creds.json, which is usually enough to prevent re-login.  
        }  
      } catch (e) {  
        console.log('Auth snapshot persist err:', e.message);  
      }  
    }  
  
    lastAuthPersistAt = Date.now();  
  } catch (e) {  
    // avoid crashing bot  
    console.log('Auth persist err:', e.message);  
  } finally {  
    authPersistInFlight = false;  
    if (authPersistPending) {  
      authPersistPending = false;  
      persistWhatsAppAuthToUpstashThrottled(true).catch(() => {});  
    }  
  }  
}  
  
async function clearWhatsAppAuthInUpstash() {  
  if (!upstashEnabled) return;  
  await redisDel(WA_AUTH_CREDS_KEY);  
  await redisDel(WA_AUTH_SNAPSHOT_KEY);  
}  
  
// ─────────────────────────────────────────// GOOGLE SHEETS// ─────────────────────────────────────────  
async function getGoogleToken() {  
  try {  
    const email = process.env.GOOGLE_CLIENT_EMAIL;  
    const key = (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n');  
    const sheetId = process.env.GOOGLE_SHEET_ID;  
    if (!email || !key || !sheetId) return null;  
  
    const now = Math.floor(Date.now() / 1000);  
  
    // JWT header/payload  
    const header = Buffer.from(  
      JSON.stringify({ alg: 'RS256', typ: 'JWT' })  
    ).toString('base64url');  
  
    const payload = Buffer.from(  
      JSON.stringify({  
        iss: email,  
        scope: 'https://www.googleapis.com/auth/spreadsheets',  
        aud: 'https://oauth2.googleapis.com/token',  
        exp: now + 3600,  
        iat: now  
      })  
    ).toString('base64url');  
  
    const sign = crypto.createSign('RSA-SHA256');  
    sign.update(`${header}.${payload}`);  
    const jwt = `${header}.${payload}.${sign.sign(key, 'base64url')}`;  
  
    const res = await axios.post(  
      'https://oauth2.googleapis.com/token',  
      {  
        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',  
        assertion: jwt  
      }  
    );  
  
    return res.data?.access_token || null;  
  } catch {  
    return null;  
  }  
}  
  
async function saveToSheet(data) {  
  try {  
    const token = await getGoogleToken();  
    if (!token) return;  
  
    const sheetId = process.env.GOOGLE_SHEET_ID;  
  
    await axios.post(  
      `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/Sheet1!A:H:append?valueInputOption=USER_ENTERED`,  
      {  
        values: [  
          [  
            data.orderId || '',  
            data.customerName || '',  
            data.customerNumber || '',  
            data.product || '',  
            data.amount || '',  
            data.status || '',  
            data.language || '',  
            new Date().toLocaleString('en-PK', {  
              timeZone: 'Asia/Karachi'  
            })  
          ]  
        ]  
      },  
      { headers: { Authorization: `Bearer ${token}` } }  
    );  
  } catch (e) {  
    console.log('Sheet error:', e.message);  
  }  
}  
  
async function initSheet() {  
  try {  
    const token = await getGoogleToken();  
    if (!token) return;  
  
    const sheetId = process.env.GOOGLE_SHEET_ID;  
  
    await axios.post(  
      `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/Sheet1!A1:append?valueInputOption=USER_ENTERED`,  
      {  
        values: [  
          [  
            'Order ID',  
            'Customer',  
            'Phone',  
            'Product',  
            'Amount',  
            'Status',  
            'Language',  
            'Date'  
          ]  
        ]  
      },  
      { headers: { Authorization: `Bearer ${token}` } }  
    );  
  } catch {  
    // ignore  
  }  
}  
  
// ─────────────────────────────────────────// VOICE TO TEXT (Groq Whisper) // ─────────────────────────────────────────  
async function voiceToText(audioBuffer) {  
  try {  
    const FormData = require('form-data');  
    const form = new FormData();  
    form.append('file', audioBuffer, { filename: 'audio.ogg', contentType: 'audio/ogg' });  
    form.append('model', 'whisper-large-v3');  
    form.append('response_format', 'json');  
  
    const res = await axios.post(  
      'https://api.groq.com/openai/v1/audio/transcriptions',  
      form,  
      {  
        headers: { ...form.getHeaders(), Authorization: `Bearer ${process.env.GROQ_API_KEY}` },  
        timeout: 30000  
      }  
    );  
  
    return res.data?.text || null;  
  } catch {  
    return null;  
  }  
}  
  
// ─────────────────────────────────────────// LANGUAGE DETECTION // ─────────────────────────────────────────  
function detectLang(text) {  
  if (/[\u0600-\u06FF]/.test(text)) return 'urdu';  
  if (/\b(kya|hai|haan|nahi|bhai|yar|chahiye|theek|acha|karo|dedo|batao|kitna|lena|mujhe|yrr)\b/i.test(text))  
    return 'roman_urdu';  
  return 'english';  
}  
  
// ─────────────────────────────────────────// DATA STORE // ─────────────────────────────────────────  
const DATA_KEY = 'bot_data_v6';  
const DATA_FILE = '/tmp/bot_data_v6.json';  
  
function getDefaultData() {  
  return {  
    settings: {  
      businessName: 'Mega Agency',  
      adminNumber: process.env.ADMIN_NUMBER || '',  
      dashboardPassword: process.env.DASHBOARD_PASSWORD || 'admin123',  
      currency: 'PKR'  
    },  
    payment: {  
      easypaisa: { number: '03XX-XXXXXXX', name: 'Tumhara Naam' },  
      jazzcash: { number: '03XX-XXXXXXX', name: 'Tumhara Naam' },  
      bank: {  
        bankName: 'HBL',  
        accountNumber: 'XXXXXXXXXXXXXXX',  
        accountName: 'Tumhara Naam',  
        iban: 'PK00XXXX0000000000000000'  
      }  
    },  
    products: [  
      {  
        id: 1,  
        name: '100+ Premium Shopify Themes Bundle',  
        price: 999,  
        description: 'Complete collection of 100+ premium themes for all niches',  
        features: [  
          '100+ Premium Themes',  
          'All Niches Covered',  
          'Fashion, Electronics, Food & More',  
          'Regular Updates',  
          '24/7 Support',  
          'Installation Guide',  
          'Mobile Optimized'  
        ],  
        downloadLink: '',  
        active: true  
      }  
    ],  
    aiPrompt: `Tum Mega Agency ke professional AI Sales Agent ho. Tumhara naam "Max" hai.  
TUMHARI SERVICE:-  
Product: 100+ Premium Shopify Themes Mega Bundle  
Price: PKR 999 ONLY (yahi final price hai — koi aur price mat batana)  
Delivery: Payment approve hone ke 1 hour baad  
Features: 100+ themes, fashion/electronics/food/all niches, regular updates, installation guide, 24/7 support  
  
LANGUAGE: Customer ki language follow karo (Urdu/Roman Urdu/English)  
TUMHARA KAAM:  
1. Customer se warmly greet karo  
2. Unke niche ke baare mein poocho  
3. Value explain karo specifically  
4. Price objections confidently handle karo  
5. Jab customer BUY karna chahe — ORDER_READY likho  
  
PRICE NEGOTIATION — IRON RULE:  
Discount KABHI NAHI — PKR 999 FINAL HAI  
"Mehenga hai" → "Ek theme 5000+ ki, 100+ sirf 999 — PKR 10 per theme!"  
"Kam karo" → "Bhai yeh already lowest — quality se compromise nahi hoga"  
  
SELLING:  
- Value: "Market mein ek theme 5000+ ki hai, 100+ sirf PKR 999"  
- Per unit: "Sirf PKR 10 per theme"  
- FOMO: "Competitors already use kar rahe hain"  
- ROI: "Ek sale se 999 wapas"  
  
RULES:  
- Short replies — 3-4 lines max  
- Friendly emojis  
- ORDER_READY bilkul start mein jab order ho`,  
    broadcasts: [],  
    orders: {},  
    customers: {},  
    orderCounter: 1000  
  };  
}  
  
let botData = getDefaultData();  
  
// Load from Upstash first, fallback to local file  
async function loadData() {  
  try {  
    const saved = upstashEnabled ? await redisGet(DATA_KEY) : null;  
    if (saved) {  
      botData = { ...getDefaultData(), ...saved };  
      botData.customers = botData.customers || {};  
      botData.broadcasts = botData.broadcasts || [];  
      console.log('✅ Data loaded from Upstash!');  
      return;  
    }  
  
    if (fs.existsSync(DATA_FILE)) {  
      const saved2 = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));  
      botData = { ...getDefaultData(), ...saved2 };  
      botData.customers = botData.customers || {};  
      botData.broadcasts = botData.broadcasts || [];  
      console.log('✅ Data loaded from local file!');  
    }  
  } catch (e) {  
    console.log('Load error:', e.message);  
  }  
}  
  
async function saveData() {  
  try {  
    if (upstashEnabled) await redisSet(DATA_KEY, botData);  
    fs.writeFileSync(DATA_FILE, JSON.stringify(botData, null, 2));  
  } catch (e) {  
    console.log('Save error:', e.message);  
  }  
}  
  
// ─────────────────────────────────────────// BOT STATE // ─────────────────────────────────────────  
let currentQR = null;  
let botStatus = 'starting';  
let sockGlobal = null;  
  
const salesHistory = {}; // keyed by customerJid  
let broadcastRunning = false;  
  
let existingChats = [];  
let chatsLoaded = false;  
let globalStore = null;  
  
// ─────────────────────────────────────────// AUTH / BODY UTILS // ─────────────────────────────────────────  
function isAuthenticated(req) {  
  const cookies = req.headers.cookie || '';  
  const sessionMatch = cookies.match(/session=([^;]+)/);  
  if (!sessionMatch) return false;  
  return sessions[sessionMatch[1]] === true;  
}  
  
async function parseBody(req) {  
  return new Promise((resolve) => {  
    let body = '';  
    req.on('data', (chunk) => (body += chunk.toString()));  
    req.on('end', () => {  
      try {  
        resolve(JSON.parse(body || '{}'));  
      } catch {  
        resolve({});  
      }  
    });  
  });  
}  
  
const sessions = {};  
  
// ─────────────────────────────────────────// CHATS PROCESSING // ─────────────────────────────────────────  
function processChatsFromStore() {  
  try {  
    if (!globalStore) {  
      chatsLoaded = true;  
      return;  
    }  
    const chats = globalStore.chats.all();  
    const newChats = [];  
  
    for (const chat of chats) {  
      if (!chat.id) continue;  
      if (chat.id.endsWith('@g.us')) continue;  
      if (chat.id.endsWith('@broadcast')) continue;  
      if (chat.id === 'status@broadcast') continue;  
      if (chat.id.includes('newsletter')) continue;  
  
      const number = chat.id.replace('@s.whatsapp.net', '');  
      if (number.length < 10) continue;  
  
      newChats.push({  
        jid: chat.id,  
        number,  
        name: chat.name || chat.pushName || number,  
        lastMessage: chat.conversationTimestamp || 0  
      });  
    }  
  
    newChats.sort((a, b) => b.lastMessage - a.lastMessage);  
    existingChats = newChats;  
    chatsLoaded = true;  
    console.log(`✅ ${newChats.length} chats processed!`);  
  } catch (e) {  
    console.log('Chat process error:', e.message);  
    chatsLoaded = true;  
  }  
}  
  
// ─────────────────────────────────────────// AI: COMMON MODEL CALLER // ─────────────────────────────────────────  
const AI_CHAT_MODELS = [  
  { provider: 'groq', model: 'llama-3.3-70b-versatile' },  
  { provider: 'groq', model: 'llama-3.1-8b-instant' },  
  { provider: 'groq', model: 'gemma2-9b-it' },  
  { provider: 'groq', model: 'llama3-70b-8192' },  
  { provider: 'openrouter', model: 'meta-llama/llama-3.1-8b-instruct:free' },  
  { provider: 'openrouter', model: 'google/gemma-2-9b-it:free' },  
  { provider: 'openrouter', model: 'mistralai/mistral-7b-instruct:free' }  
];  
  
function getLangRule(lang) {  
  if (lang === 'urdu') return 'Sirf Urdu script mein reply karo.';  
  if (lang === 'roman_urdu') return 'Roman Urdu mein reply karo.';  
  return 'English mein reply karo.';  
}  
  
async function callLLMChatCompletions(messages, { temperature = 0.85, max_tokens = 350 } = {}) {  
  for (const { provider, model } of AI_CHAT_MODELS) {  
    try {  
      if (provider === 'groq' && !process.env.GROQ_API_KEY) continue;  
      if (provider === 'openrouter' && !process.env.OPENROUTER_API_KEY) continue;  
  
      const apiUrl =  
        provider === 'groq'  
          ? 'https://api.groq.com/openai/v1/chat/completions'  
          : 'https://openrouter.ai/api/v1/chat/completions';  
  
      const headers =  
        provider === 'groq'  
          ? { Authorization: `Bearer ${process.env.GROQ_API_KEY}`, 'Content-Type': 'application/json' }  
          : {  
              Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`,  
              'Content-Type': 'application/json',  
              'HTTP-Referer': 'https://mega-agency.com',  
              'X-Title': 'Mega Agency'  
            };  
  
      const response = await axios.post(  
        apiUrl,  
        { model, messages, max_tokens, temperature },  
        { headers, timeout: 15000 }  
      );  
  
      const content = response.data?.choices?.[0]?.message?.content;  
      if (content && content.trim()) {  
        return { content: content.trim(), provider, model };  
      }  
    } catch {  
      // try next model  
    }  
  }  
  return null;  
}  
  
function getActiveProduct() {  
  return botData.products.find((p) => p.active) || botData.products[0];  
}  
  
// ─────────────────────────────────────────// AI SALES RESPONSE (ORDER_READY detection kept) // ─────────────────────────────────────────  
async function getAISalesResponse(userMessage, userId, customerName, lang) {  
  try {  
    if (!salesHistory[userId]) salesHistory[userId] = [];  
  
    salesHistory[userId].push({ role: 'user', content: userMessage });  
    if (salesHistory[userId].length > 30) salesHistory[userId] = salesHistory[userId].slice(-30);  
  
    const activeProduct = getActiveProduct();  
    const langRule = getLangRule(lang);  
  
    const systemPrompt =  
      botData.aiPrompt +  
      `\n\n${langRule}` +  
      `\nCustomer naam: ${customerName}` +  
      `\nActive Product: ${activeProduct.name}` +  
      `\nPrice: ${botData.settings.currency} ${activeProduct.price}` +  
      `\nYAD RAKHO: Price kabhi kam nahi karo!`;  
  
    const result = await callLLMChatCompletions(  
      [{ role: 'system', content: systemPrompt }, ...salesHistory[userId]],  
      { temperature: 0.85, max_tokens: 350 }  
    );  
  
    if (!result?.content) {  
      // cleanup the pushed user message  
      if (salesHistory[userId]?.length) salesHistory[userId].pop();  
      const fb = {  
        urdu: '⚠️ تکنیکی مسئلہ — 1 منٹ بعد کوشش کریں! 🙏',  
        roman_urdu: '⚠️ Thodi technical difficulty. 1 min mein dobara try karo! 🙏',  
        english: '⚠️ Technical issue. Try again in 1 minute! 🙏'  
      };  
      return { message: fb[lang] || fb.roman_urdu, shouldOrder: false, product: activeProduct };  
    }  
  
    const aiMessage = result.content;  
    salesHistory[userId].push({ role: 'assistant', content: aiMessage });  
  
    const shouldOrder = aiMessage.toUpperCase().includes('ORDER_READY');  
    const cleanMessage = aiMessage.replace(/ORDER_READY/gi, '').trim();  
  
    console.log(`✅ AI: ${result.provider}/${result.model} | ${lang}`);  
    return { message: cleanMessage, shouldOrder, product: activeProduct };  
  } catch {  
    const activeProduct = getActiveProduct();  
    const fb = {  
      urdu: '⚠️ تکنیکی مسئلہ — 1 منٹ بعد کوشش کریں! 🙏',  
      roman_urdu: '⚠️ Thodi technical difficulty. 1 min mein dobara try karo! 🙏',  
      english: '⚠️ Technical issue. Try again in 1 minute! 🙏'  
    };  
    return { message: fb[lang] || fb.roman_urdu, shouldOrder: false, product: activeProduct };  
  }  
}  
  
// ─────────────────────────────────────────// STATIC FALLBACK MESSAGES (used if AI fails validation) // ─────────────────────────────────────────  
function getPaymentMessage(orderId, product, lang) {  
  const p = botData.payment;  
  const details = `━━━━━━━━━━━━━━━━━━━━💳 *Payment — ${botData.settings.currency} ${product.price}*  
📱 *EasyPaisa:* Number: ${p.easypaisa.number}  
Name: ${p.easypaisa.name}  
📱 *JazzCash:* Number: ${p.jazzcash.number}  
Name: ${p.jazzcash.name}  
🏦 *Bank Transfer:* Bank: ${p.bank.bankName}  
Account: ${p.bank.accountNumber}  
Name: ${p.bank.accountName}  
IBAN: ${p.bank.iban}  
━━━━━━━━━━━━━━━━━━━━`;  
  
  if (lang === 'urdu') {  
    return `🛒 *آرڈر کنفرم! #${orderId}*\n\n${details}\n\n✅ پیمنٹ کے بعد اسکرین شاٹ بھیجیں\n⏳ 1 گھنٹے میں ڈلیوری!`;  
  }  
  if (lang === 'roman_urdu') {  
    return `🛒 *Order Confirmed! #${orderId}*\nProduct: *${product.name}*\n\n${details}\n\n✅ Payment ke baad *screenshot* bhejo\n📦 1 hour mein delivery guaranteed!`;  
  }  
  return `🛒 *Order Confirmed! #${orderId}*\nProduct: *${product.name}*\n\n${details}\n\n✅ Send screenshot after payment\n📦 Delivery within 1 hour!`;  
}  
  
function getVoiceErrorStatic(lang) {  
  if (lang === 'urdu') return `🎤 آپ کی آواز سمجھ نہیں آئی۔ براہِ کرم ٹیکسٹ میں لکھیں یا "buy" بھیجیں 🙏`;  
  if (lang === 'roman_urdu') return `🎤 Aap ki voice samajh nahi aayi. Please text mein likhein ya "buy" bhejein 🙏`;  
  return `🎤 I couldn't understand the voice. Please type your message or send "buy" 🙏`;  
}  
  
function getScreenshotReceivedStatic(orderId, lang) {  
  const msgs = {  
    urdu: `📸 *اسکرین شاٹ موصول!*\n\nآرڈر *#${orderId}*\n✅ ایڈمن تصدیق کر رہا ہے\n⏳ 1 گھنٹے میں! 🙏`,  
    roman_urdu: `📸 *Screenshot Receive Ho Gaya!*\n\nOrder *#${orderId}*\n✅ Admin verify kar raha hai\n⏳ 1 hour mein themes deliver honge!\n\nShukriya! 🙏`,  
    english: `📸 *Screenshot Received!*\n\nOrder *#${orderId}*\n✅ Admin is verifying\n⏳ Delivery within 1 hour!\n\nThank you! 🙏`  
  };  
  return msgs[lang] || msgs.roman_urdu;  
}  
  
function getScreenshotNoOrderStatic(lang) {  
  const msgs = {  
    urdu: `📸 Screenshot موصول ہو گیا!\n\nلیکن کوئی pending آرڈر نہیں ملا۔\nآرڈر start کرنے کیلئے براہِ کرم "buy" لکھیں 🙏`,  
    roman_urdu: `📸 Screenshot mil gaya!\n\nLekin koi pending order nahi mila.\nOrder start karne ke liye please "buy" likho 🙏`,  
    english: `📸 Screenshot received!\n\nBut I couldn't find a pending order.\nPlease type "buy" to start your order 🙏`  
  };  
  return msgs[lang] || msgs.roman_urdu;  
}  
  
function getPaymentApprovedStatic(order, product, lang) {  
  const businessName = botData.settings.businessName || 'Mega Agency';  
  const downloadBlock = product.downloadLink  
    ? `⬇️ *Download Link:*\n${product.downloadLink}\n\n`  
    : '';  
  
  if (lang === 'urdu') {  
    return `🎉 *پیمنٹ کنفرم!*\n\nآرڈر *#${order.orderId}* کنفرم ہو گیا!\n\n📦 *${product.name}*\n\n${downloadBlock}مدد چاہیے تو میسج کریں!\nشکریہ ${businessName} کو choose کرنے کا! 🙏`;  
  }  
  if (lang === 'roman_urdu') {  
    return `🎉 *Payment Approved!*\n\nOrder *#${order.orderId}* confirm ho gaya!\n\n📦 *${product.name}*\n\n${downloadBlock}Koi help chahiye? toh message karo!\nShukriya ${businessName} ko choose karne ka! 🙏`;  
  }  
  return `🎉 *Payment Approved!*\n\nOrder *#${order.orderId}* confirmed!\n\n📦 *${product.name}*\n\n${downloadBlock}Any help needed? Message us!\nThanks for choosing ${businessName}! 🙏`;  
}  
  
function getPaymentRejectedStatic(order, lang) {  
  if (lang === 'urdu') {  
    return `❌ *پیمنٹ ویریفائی نہیں ہو سکی*\n\nآرڈر *#${order.orderId}*\n\nScreenshot واضح نہیں تھا.\nبراہِ کرم دوبارہ سہی screenshot بھیجیں یا admin سے رابطہ کریں.\n\n"buy" لکھ کر دوبارہ try کریں 💪`;  
  }  
  if (lang === 'roman_urdu') {  
    return `❌ *Payment Verify Nahi Ho Saki*\n\nOrder *#${order.orderId}*\n\nScreenshot sahi nahi tha.\nDobara sahi screenshot bhejo ya admin se contact karo.\n\n"buy" likhkar dobara try karo! 💪`;  
  }  
  return `❌ *Payment Verify Failed*\n\nOrder *#${order.orderId}*\n\nScreenshot wasn't clear.\nPlease resend a correct screenshot or contact admin.\n\nType "buy" to try again 💪`;  
}  
  
function digitsOnly(str) {  
  return (str || '').toString().replace(/\D/g, '');  
}  
  
function validatePaymentMessage(text, orderId) {  
  try {  
    const p = botData.payment;  
    const msgDigits = digitsOnly(text);  
    const idDigits = digitsOnly(String(orderId));  
    const hasOrderId = idDigits && msgDigits.includes(idDigits);  
  
    const candidates = [  
      p.easypaisa?.number,  
      p.jazzcash?.number,  
      p.bank?.accountNumber,  
      p.bank?.iban  
    ].filter(Boolean);  
  
    const hasPayment =  
      candidates.some((n) => {  
        const dn = digitsOnly(n);  
        if (!dn) return false;  
        const part = dn.slice(-Math.min(10, dn.length)); // compare last chunk  
        return part && msgDigits.includes(part);  
      });  
  
    return hasOrderId && hasPayment;  
  } catch {  
    return false;  
  }  
}  
  
// ─────────────────────────────────────────// AI SCENARIO GENERATOR (all bot replies go through AI) // ─────────────────────────────────────────  
async function getAIReplyScenario({  
  userId,  
  customerName,  
  lang,  
  product,  
  scenarioName,  
  scenarioInstructions,  
  userPrompt,  
  fallbackText,  
  validateFn,  
  temperature = 0.45,  
  max_tokens = 450  
}) {  
  if (!salesHistory[userId]) salesHistory[userId] = [];  
  
  const activeProduct = product || getActiveProduct();  
  const langRule = getLangRule(lang);  
  
  const systemPrompt =  
    botData.aiPrompt +  
    `\n\n${langRule}` +  
    `\nCustomer naam: ${customerName}` +  
    `\nActive Product: ${activeProduct?.name || ''}` +  
    `\nPrice: ${botData.settings.currency} ${activeProduct?.price || ''}` +  
    `\nYAD RAKHO: Price kabhi kam nahi karo!` +  
    `\n\nSCENARIO: ${scenarioName}` +  
    `\n${scenarioInstructions}` +  
    `\nIMPORTANT: ORDER_READY word bilkul mat likhna. If it appears, remove it.`; // override  
  
  const history = (salesHistory[userId] || []).slice(-20);  
  
  const messages = [  
    { role: 'system', content: systemPrompt },  
    ...history,  
    { role: 'user', content: userPrompt }  
  ];  
  
  const result = await callLLMChatCompletions(messages, { temperature, max_tokens });  
  
  const finalFallback = fallbackText;  
  
  if (!result?.content) return finalFallback;  
  
  let text = result.content.trim().replace(/ORDER_READY/gi, '').trim();  
  if (validateFn && !validateFn(text)) return finalFallback;  
  
  salesHistory[userId].push({ role: 'assistant', content: text });  
  if (salesHistory[userId].length > 30) salesHistory[userId] = salesHistory[userId].slice(-30);  
  
  return text;  
}  
  
async function getVoiceErrorMessageAI(userId, customerName, lang) {  
  const fallbackText = getVoiceErrorStatic(lang);  
  return (  
    (await getAIReplyScenario({  
      userId,  
      customerName,  
      lang,  
      product: getActiveProduct(),  
      scenarioName: 'VOICE_UNDERSTAND_ERROR',  
      scenarioInstructions:  
        'Customer ne voice bheji lekin text samajh nahi aaya. Friendly, short (max 3-4 lines) message do. Ask customer to type text. Mention "buy" as a quick option.',  
      userPrompt: 'VOICE WAS NOT UNDERSTOOD. GENERATE ERROR + NEXT-STEP MESSAGE.',  
      fallbackText  
    })) || fallbackText  
  );  
}  
  
async function getScreenshotReceivedMessageAI({ userId, customerName, lang, orderId, product }) {  
  const fallbackText = getScreenshotReceivedStatic(orderId, lang);  
  return (  
    (await getAIReplyScenario({  
      userId,  
      customerName,  
      lang,  
      product,  
      scenarioName: 'PAYMENT_SCREENSHOT_RECEIVED',  
      scenarioInstructions:  
        `Customer ne payment screenshot bhej diya hai for Order #${orderId}. Confirm receipt. Admin verify + delivery within 1 hour mention karo. Short (3-5 lines). Emojis. Do NOT include payment numbers again.`,  
      userPrompt: `OrderId: ${orderId}\nPayment screenshot received. Please respond accordingly.`,  
      fallbackText,  
      validateFn: (t) => digitsOnly(t).includes(digitsOnly(orderId))  
    })) || fallbackText  
  );  
}  
  
async function getScreenshotNoOrderMessageAI({ userId, customerName, lang }) {  
  const fallbackText = getScreenshotNoOrderStatic(lang);  
  return (  
    (await getAIReplyScenario({  
      userId,  
      customerName,  
      lang,  
      product: getActiveProduct(),  
      scenarioName: 'SCREENSHOT_RECEIVED_NO_PENDING_ORDER',  
      scenarioInstructions:  
        'Customer ne screenshot bhej diya lekin pending order nahi mila. Customer ko "buy" likhne ko bolo taake order start ho. Friendly and short (3-4 lines).',  
      userPrompt: 'Screenshot received but no pending order exists in system.',  
      fallbackText  
    })) || fallbackText  
  );  
}  
  
async function getPaymentMessageAI({ userId, customerName, lang, orderId, product }) {  
  const fallbackText = getPaymentMessage(orderId, product, lang);  
  
  const p = botData.payment;  
  const paymentBlock = `EasyPaisa: ${p.easypaisa.number} (Name: ${p.easypaisa.name})  
JazzCash: ${p.jazzcash.number} (Name: ${p.jazzcash.name})  
Bank: ${p.bank.bankName}, Account: ${p.bank.accountNumber}, Name: ${p.bank.accountName}, IBAN: ${p.bank.iban}`;  
  
  const scenarioInstructions =  
    `This is PAYMENT REQUEST step. Must include payment instructions and ask customer to send screenshot after payment.\n` +  
    `FINAL PRICE ONLY: ${botData.settings.currency} ${product.price}\n` +  
    `Do NOT ask niche questions. Do NOT output ORDER_READY.\n` +  
    `Keep message clear with emojis. Mention delivery within 1 hour.\n` +  
    `Include payment details EXACTLY (numbers must match).`;  
  
  const userPrompt =  
    `Order Confirmed.\nOrderId: ${orderId}\nProduct: ${product.name}\nPrice: ${botData.settings.currency} ${product.price}\n` +  
    `Payment details (must include):\n${paymentBlock}\n` +  
    `Now write the WhatsApp message in the customer's language: ${lang}.`;  
  
  const validateFn = (text) => validatePaymentMessage(text, orderId);  
  
  try {  
    const aiText = await getAIReplyScenario({  
      userId,  
      customerName,  
      lang,  
      product,  
      scenarioName: 'PAYMENT_REQUEST',  
      scenarioInstructions,  
      userPrompt,  
      fallbackText,  
      validateFn,  
      temperature: 0.35,  
      max_tokens: 520  
    });  
    return aiText || fallbackText;  
  } catch {  
    return fallbackText;  
  }  
}  
  
async function getPaymentApprovedMessageAI({ userId, customerName, lang, order, product }) {  
  const fallbackText = getPaymentApprovedStatic(order, product, lang);  
  
  const scenarioInstructions =  
    `PAYMENT APPROVED step.\n` +  
    `Confirm order is confirmed for Order #${order.orderId}.\n` +  
    `Mention product name.\n` +  
    `If downloadLink exists, include it.\n` +  
    `Short friendly message. Do NOT output ORDER_READY.`;  
  
  const userPrompt =  
    `OrderId: ${order.orderId}\nProduct: ${product.name}\nDownloadLink: ${product.downloadLink || ''}\nBusiness: ${botData.settings.businessName}\n` +  
    `Generate approved message in language: ${lang}.`;  
  
  const validateFn = (t) => {  
    const okOrder = digitsOnly(t).includes(digitsOnly(order.orderId));  
    const okProduct = product?.name ? t.includes(product.name) || t.toLowerCase().includes(product.name.toLowerCase().slice(0, 8)) : true;  
    const okDownload = product?.downloadLink ? t.includes(product.downloadLink) : true;  
    return okOrder && okProduct && okDownload;  
  };  
  
  try {  
    const aiText = await getAIReplyScenario({  
      userId,  
      customerName,  
      lang,  
      product,  
      scenarioName: 'PAYMENT_APPROVED',  
      scenarioInstructions,  
      userPrompt,  
      fallbackText,  
      validateFn,  
      temperature: 0.35,  
      max_tokens: 480  
    });  
    return aiText || fallbackText;  
  } catch {  
    return fallbackText;  
  }  
}  
  
async function getPaymentRejectedMessageAI({ userId, customerName, lang, order }) {  
  const fallbackText = getPaymentRejectedStatic(order, lang);  
  
  const scenarioInstructions =  
    `PAYMENT REJECTED step.\n` +  
    `Order #${order.orderId} screenshot not verified. Apologize and ask customer to resend correct screenshot or contact admin.\n` +  
    `Mention instruction: type "buy" to try again.\n` +  
    `Short friendly message. Do NOT output ORDER_READY.`;  
  
  const userPrompt =  
    `OrderId: ${order.orderId}\nGenerate rejected message in language: ${lang}.`;  
  
  const validateFn = (t) => digitsOnly(t).includes(digitsOnly(order.orderId));  
  
  try {  
    const aiText = await getAIReplyScenario({  
      userId,  
      customerName,  
      lang,  
      product: getActiveProduct(),  
      scenarioName: 'PAYMENT_REJECTED',  
      scenarioInstructions,  
      userPrompt,  
      fallbackText,  
      validateFn,  
      temperature: 0.45,  
      max_tokens: 420  
    });  
    return aiText || fallbackText;  
  } catch {  
    return fallbackText;  
  }  
}  
  
// ─────────────────────────────────────────// DASHBOARD: AI BROADCAST GENERATOR (kept) // ─────────────────────────────────────────  
async function generateBroadcastMessage(offerDetails, customerName, personalized) {  
  const models = [  
    { provider: 'groq', model: 'llama-3.3-70b-versatile' },  
    { provider: 'groq', model: 'llama-3.1-8b-instant' },  
    { provider: 'openrouter', model: 'meta-llama/llama-3.1-8b-instruct:free' }  
  ];  
  
  const prompt = personalized  
    ? `WhatsApp marketing message likho "${customerName}" ke liye.\nOffer: ${offerDetails}\nRules: Roman Urdu, 3-5 lines, compelling, naam use karo, emojis, price clear karo, call to action.`  
    : `WhatsApp marketing message likho.\nOffer: ${offerDetails}\nRules: Roman Urdu, 3-5 lines, compelling, emojis, price clear karo, call to action.`;  
  
  for (const { provider, model } of models) {  
    try {  
      const apiUrl =  
        provider === 'groq'  
          ? 'https://api.groq.com/openai/v1/chat/completions'  
          : 'https://openrouter.ai/api/v1/chat/completions';  
  
      const headers =  
        provider === 'groq'  
          ? {  
              Authorization: `Bearer ${process.env.GROQ_API_KEY}`,  
              'Content-Type': 'application/json'  
            }  
          : {  
              Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`,  
              'Content-Type': 'application/json',  
              'HTTP-Referer': 'https://mega-agency.com',  
              'X-Title': 'Mega Agency'  
            };  
  
      const res = await axios.post(  
        apiUrl,  
        {  
          model,  
          messages: [{ role: 'user', content: prompt }],  
          max_tokens: 200,  
          temperature: 0.9  
        },  
        { headers, timeout: 15000 }  
      );  
  
      return res.data?.choices?.[0]?.message?.content?.trim() || offerDetails;  
    } catch {  
      // next  
    }  
  }  
  return offerDetails;  
}  
  
async function runBroadcast(broadcast) {  
  if (!sockGlobal) return;  
  
  broadcastRunning = true;  
  
  const targets = broadcast.selectedContacts || [];  
  let sent = 0;  
  let failed = 0;  
  
  broadcast.status = 'running';  
  broadcast.sentCount = 0;  
  broadcast.failedCount = 0;  
  
  await saveData();  
  
  for (const contact of targets) {  
    try {  
      let message = broadcast.baseMessage;  
  
      if (broadcast.personalized && broadcast.offerDetails) {  
        message = await generateBroadcastMessage(  
          broadcast.offerDetails,  
          contact.name || 'Dost',  
          true  
        );  
      }  
  
      await sockGlobal.sendMessage(contact.jid, { text: message });  
  
      sent++;  
      broadcast.sentCount = sent;  
      console.log(`📤 Sent ${sent}/${targets.length} → ${contact.name || contact.number}`);  
  
      await new Promise((r) => setTimeout(r, (broadcast.delaySeconds || 5) * 1000));  
    } catch {  
      failed++;  
      broadcast.failedCount = failed;  
    }  
  }  
  
  broadcast.status = 'completed';  
  broadcast.completedAt = Date.now();  
  await saveData();  
  
  broadcastRunning = false;  
  console.log(`✅ Broadcast done! Sent:${sent} Failed:${failed}`);  
}  
  
// ─────────────────────────────────────────// MESSAGE HANDLER (ALL CUSTOMER REPLIES VIA AI) // ─────────────────────────────────────────  
async function handleMessage(sock, message) {  
  try {  
    if (message.key.fromMe) return;  
  
    const senderId = message.key?.remoteJid;  
    if (!senderId) return;  
  
    if (senderId === 'status@broadcast') return;  
    if (senderId.endsWith('@broadcast')) return;  
    if (senderId.includes('newsletter')) return;  
    if (senderId.endsWith('@g.us')) return;  
  
    const senderName = message.pushName || 'Customer';  
    const msgType = Object.keys(message.message || {})[0];  
  
    // Save/update customer  
    if (!botData.customers) botData.customers = {};  
    botData.customers[senderId] = botData.customers[senderId] || {  
      jid: senderId,  
      number: senderId.replace('@s.whatsapp.net', ''),  
      name: senderName,  
      lastSeen: Date.now(),  
      language: 'roman_urdu'  
    };  
    botData.customers[senderId].jid = senderId;  
    botData.customers[senderId].number = senderId.replace('@s.whatsapp.net', '');  
    botData.customers[senderId].name = senderName;  
    botData.customers[senderId].lastSeen = Date.now();  
  
    // VOICE  
    if (msgType === 'audioMessage' || msgType === 'pttMessage') {  
      const currentLang = botData.customers[senderId]?.language || 'roman_urdu';  
      await sock.sendPresenceUpdate('composing', senderId);  
  
      try {  
        const buf = await downloadMediaMessage(message, 'buffer', {});  
        const text = await voiceToText(buf);  
  
        if (text && text.trim()) {  
          const lang = detectLang(text);  
          botData.customers[senderId].language = lang;  
          await saveData();  
  
          const ai = await getAISalesResponse(text, senderId, senderName, lang);  
  
          await sock.sendPresenceUpdate('paused', senderId);  
  
          // AI reply (no static prefix so this reply is fully AI-driven)  
          if (ai.message && ai.message.trim()) {  
            await sock.sendMessage(senderId, { text: ai.message }, { quoted: message });  
            await new Promise((r) => setTimeout(r, 600));  
          }  
  
          if (ai.shouldOrder) {  
            botData.orderCounter++;  
            const orderId = botData.orderCounter;  
  
            const product = ai.product || getActiveProduct();  
            botData.orders[senderId] = {  
              orderId,  
              customerJid: senderId,  
              customerNumber: senderId.replace('@s.whatsapp.net', ''),  
              customerName: senderName,  
              productId: product?.id,  
              language: lang,  
              status: 'pending',  
              hasScreenshot: false,  
              timestamp: Date.now()  
            };  
  
            await saveData();  
  
            const paymentMsg = await getPaymentMessageAI({  
              userId: senderId,  
              customerName: senderName,  
              lang,  
              orderId,  
              product  
            });  
  
            await sock.sendMessage(senderId, { text: paymentMsg });  
            console.log(`🛒 New Order: #${orderId} for ${senderName}`);  
            await saveToSheet({  
              orderId,  
              customerName: senderName,  
              customerNumber: senderId.replace('@s.whatsapp.net', ''),  
              product: product?.name,  
              amount: product?.price,  
              status: 'pending',  
              language: lang  
            });  
          }  
  
          return;  
        }  
  
        // voice not understood -> AI error message  
        const errMsg = await getVoiceErrorMessageAI(senderId, senderName, currentLang);  
        await sock.sendPresenceUpdate('paused', senderId);  
        await sock.sendMessage(senderId, { text: errMsg }, { quoted: message });  
      } catch {  
        const errMsg = await getVoiceErrorMessageAI(senderId, senderName, currentLang);  
        await sock.sendPresenceUpdate('paused', senderId);  
        await sock.sendMessage(senderId, { text: errMsg }, { quoted: message });  
      }  
      return;  
    }  
  
    // IMAGE (screenshot)  
    if (msgType === 'imageMessage') {  
      const existingOrder = Object.values(botData.orders || {}).find(  
        (o) => o.customerJid === senderId && o.status === 'pending'  
      );  
  
      const lang = botData.customers[senderId]?.language || 'roman_urdu';  
  
      if (existingOrder) {  
        existingOrder.hasScreenshot = true;  
        await saveData();  
  
        const product =  
          botData.products.find((p) => p.id === existingOrder.productId) || botData.products[0];  
  
        const aiMsg = await getScreenshotReceivedMessageAI({  
          userId: senderId,  
          customerName: senderName,  
          lang,  
          orderId: existingOrder.orderId,  
          product  
        });  
  
        await sock.sendMessage(senderId, { text: aiMsg });  
  
        // Admin notification (internal - not requested as AI)  
        const adminJid = botData.settings.adminNumber  
          ? `${botData.settings.adminNumber}@s.whatsapp.net`  
          : null;  
  
        if (adminJid) {  
          try {  
            await sock.sendMessage(adminJid, {  
              text: `🔔 New Payment Screenshot!\n\nOrder: *#${existingOrder.orderId}*\nCustomer: ${senderName}\nNumber: ${existingOrder.customerNumber}\n\nDashboard pe approve/reject karo! ⚡`  
            });  
          } catch {  
            // ignore  
          }  
        }  
      } else {  
        const aiMsg = await getScreenshotNoOrderMessageAI({  
          userId: senderId,  
          customerName: senderName,  
          lang  
        });  
        await sock.sendMessage(senderId, { text: aiMsg }, { quoted: message });  
      }  
      return;  
    }  
  
    // TEXT  
    const userMessage =  
      message.message?.conversation ||  
      message.message?.extendedTextMessage?.text ||  
      '';  
  
    if (!userMessage.trim()) return;  
  
    const lang = detectLang(userMessage);  
    botData.customers[senderId].language = lang;  
    await saveData();  
  
    console.log(`📩 ${senderName}[${lang}]: ${userMessage}`);  
  
    await sock.sendPresenceUpdate('composing', senderId);  
  
    const aiReply = await getAISalesResponse(userMessage, senderId, senderName, lang);  
  
    await sock.sendPresenceUpdate('paused', senderId);  
  
    if (aiReply.shouldOrder) {  
      botData.orderCounter++;  
      const orderId = botData.orderCounter;  
  
      const product = aiReply.product || getActiveProduct();  
  
      botData.orders[senderId] = {  
        orderId,  
        customerJid: senderId,  
        customerNumber: senderId.replace('@s.whatsapp.net', ''),  
        customerName: senderName,  
        productId: product?.id,  
        language: lang,  
        status: 'pending',  
        hasScreenshot: false,  
        timestamp: Date.now()  
      };  
  
      await saveData();  
      await saveToSheet({  
        orderId,  
        customerName: senderName,  
        customerNumber: senderId.replace('@s.whatsapp.net', ''),  
        product: product?.name,  
        amount: product?.price,  
        status: 'pending',  
        language: lang  
      });  
  
      if (aiReply.message && aiReply.message.trim()) {  
        await sock.sendMessage(senderId, { text: aiReply.message }, { quoted: message });  
        await new Promise((r) => setTimeout(r, 800));  
      }  
  
      const paymentMsg = await getPaymentMessageAI({  
        userId: senderId,  
        customerName: senderName,  
        lang,  
        orderId,  
        product  
      });  
  
      await sock.sendMessage(senderId, { text: paymentMsg });  
      console.log(`🛒 New Order: #${orderId} for ${senderName}`);  
    } else {  
      if (aiReply.message && aiReply.message.trim()) {  
        await sock.sendMessage(senderId, { text: aiReply.message }, { quoted: message });  
      }  
    }  
  } catch (err) {  
    console.error('Handle error:', err?.message || err);  
  }  
}  
  
// ─────────────────────────────────────────// WHATSAPP BOT START // ─────────────────────────────────────────  
async function startBot() {  
  try {  
    const { version, isLatest } = await fetchLatestBaileysVersion();  
    console.log(`📱 WA Version: ${version.join('.')} — Latest: ${isLatest}`);  
  
    await restoreWhatsAppAuthFromUpstash();  
  
    ensureDirSync(WA_AUTH_DIR);  
  
    const { state, saveCreds: originalSaveCreds } = await useMultiFileAuthState(WA_AUTH_DIR);  
  
    globalStore = makeInMemoryStore({ logger: pino({ level: 'silent' }) });  
  
    const sock = makeWASocket({  
      version,  
      auth: state,  
      logger: pino({ level: 'silent' }),  
      browser: Browsers.ubuntu('Chrome'),  
      connectTimeoutMs: 60000,  
      defaultQueryTimeoutMs: 60000,  
      keepAliveIntervalMs: 30000,  
      emitOwnEvents: false,  
      markOnlineOnConnect: false,  
      generateHighQualityLinkPreview: false,  
      qrTimeout: 60000,  
      retryRequestDelayMs: 2000,  
      maxMsgRetryCount: 5,  
      fireInitQueries: true,  
      syncFullHistory: false  
    });  
  
    globalStore.bind(sock.ev);  
    sockGlobal = sock;  
  
    sock.ev.on('creds.update', async () => {  
      await originalSaveCreds();  
      await persistWhatsAppAuthToUpstashThrottled(false);  
    });  
  
    sock.ev.on('connection.update', async (update) => {  
      const { connection, lastDisconnect, qr } = update;  
  
      if (qr) {  
        currentQR = qr;  
        botStatus = 'qr_ready';  
        console.log('✅ QR Ready! /qr pe jao scan karne ke liye!');  
      }  
  
      if (connection === 'close') {  
        currentQR = null;  
        const code = lastDisconnect?.error?.output?.statusCode;  
  
        console.log('❌ Disconnected, code:', code);  
  
        if (code === DisconnectReason.loggedOut) {  
          botStatus = 'logged_out';  
  
          try {  
            fs.rmSync(WA_AUTH_DIR, { recursive: true, force: true });  
          } catch {}  
  
          try {  
            await clearWhatsAppAuthInUpstash();  
          } catch {}  
  
          setTimeout(startBot, 5000);  
        } else {  
          botStatus = 'reconnecting';  
          setTimeout(startBot, code === 405 ? 15000 : 10000);  
        }  
      }  
  
      if (connection === 'open') {  
        currentQR = null;  
        botStatus = 'connected';  
        console.log('✅ WhatsApp Connected! Mega Agency LIVE!');  
  
        setTimeout(processChatsFromStore, 5000);  
        await initSheet().catch(() => {});  
      }  
    });  
  
    sock.ev.on('chats.upsert', () => processChatsFromStore());  
    sock.ev.on('chats.set', () => setTimeout(processChatsFromStore, 2000));  
  
    sock.ev.on('messages.upsert', async ({ messages, type }) => {  
      if (type !== 'notify') return;  
      for (const msg of messages) {  
        await handleMessage(sock, msg);  
      }  
    });  
  } catch (err) {  
    console.error('Bot error:', err?.message || err);  
    setTimeout(startBot, 15000);  
  }  
}  
  
// ─────────────────────────────────────────// SERVER + DASHBOARD // ─────────────────────────────────────────  
function dashboardHtml() {  
  const biz = botData?.settings?.businessName || 'Mega Agency';  
  
  return `<!doctype html>  
<html>  
<head>  
<meta charset="utf-8"/>  
<meta name="viewport" content="width=device-width,initial-scale=1"/>  
<title>${biz} - Admin</title>  
<style>  
  body{background:#0a0a0a;color:#e0e0e0;font-family:Arial,sans-serif;margin:0;padding:0}  
  .wrap{max-width:1100px;margin:0 auto;padding:16px}  
  h1{margin:0 0 10px;color:#25D366;font-size:20px}  
  .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}  
  .card{background:#111;border:1px solid #222;border-radius:12px;padding:16px;margin:12px 0}  
  .badge{padding:4px 10px;border-radius:999px;font-size:12px;font-weight:bold;border:1px solid #333}  
  .badge.live{color:#25D366;border-color:#25D366}  
  .badge.off{color:#e74c3c;border-color:#e74c3c}  
  button{background:#25D366;color:black;border:none;border-radius:10px;padding:10px 14px;font-weight:bold;cursor:pointer}  
  button.gray{background:#333;color:white}  
  button.red{background:#e74c3c;color:white}  
  button.blue{background:#3498db;color:white}  
  input,textarea,select{width:100%;background:#0f0f0f;border:1px solid #333;border-radius:8px;color:white;padding:10px;margin-top:6px;box-sizing:border-box}  
  textarea{min-height:140px}  
  label{color:#aaa;font-size:13px}  
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px}  
  .order{border:1px solid #222;background:#0f0f0f;border-radius:10px;padding:12px;margin:10px 0}  
  .order .top{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}  
  .meta{color:#aaa;font-size:13px;line-height:1.6;margin-top:6px;white-space:pre-wrap}  
  .chats{max-height:260px;overflow:auto;border:1px solid #222;border-radius:10px;padding:10px;background:#0f0f0f}  
  .chatItem{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:8px;border-radius:8px}  
  .chatItem:hover{background:#161616}  
  .toast{position:fixed;bottom:16px;right:16px;background:#25D366;color:black;padding:12px 16px;border-radius:10px;font-weight:bold;display:none;z-index:999}  
  .muted{color:#888}  
  hr{border:none;border-top:1px solid #222;margin:14px 0}  
</style>  
</head>  
<body>  
<div class="wrap">  
  <h1>🏪 ${biz} Admin Panel</h1>  
  
  <div class="row">  
    <span id="botBadge" class="badge off">Loading...</span>  
    <div id="statsLine" class="muted"></div>  
    <button class="gray" id="btnReload">🔄 reload</button>  
  </div>  
  
  <div class="card">  
    <h2 style="margin:0 0 10px">📦 Orders</h2>  
    <div class="grid">  
      <div><h3>Pending</h3><div id="pendingList"></div></div>  
      <div><h3>Approved</h3><div id="approvedList"></div></div>  
      <div><h3>Rejected</h3><div id="rejectedList"></div></div>  
    </div>  
  </div>  
  
  <div class="card">  
    <h2 style="margin:0 0 10px">🤖 AI Prompt</h2>  
    <textarea id="aiPrompt"></textarea>  
    <div class="row" style="margin-top:10px"><button id="btnSavePrompt">💾 Save Prompt</button></div>  
  </div>  
  
  <div class="card">  
    <h2 style="margin:0 0 10px">⚙️ Settings</h2>  
    <div class="grid">  
      <div>  
        <label>Business Name</label>  
        <input id="s_bizName" />  
      </div>  
      <div>  
        <label>Admin WhatsApp Number</label>  
        <input id="s_adminNum" placeholder="923001234567"/>  
      </div>  
    </div>  
    <div>  
      <label>Dashboard Password (leave empty to keep current)</label>  
      <input id="s_password" type="password" placeholder="New password..."/>  
    </div>  
    <div class="row" style="margin-top:10px"><button id="btnSaveSettings">💾 Save Settings</button></div>  
  </div>  
  
  <div class="card">  
    <h2 style="margin:0 0 10px">💳 Payment Details</h2>  
    <div class="grid">  
      <div>  
        <h3>EasyPaisa</h3>  
        <label>Number</label><input id="ep_number" placeholder="03XX-XXXXXXX"/>  
        <label>Account Name</label><input id="ep_name" placeholder="Tumhara Naam"/>  
      </div>  
      <div>  
        <h3>JazzCash</h3>  
        <label>Number</label><input id="jc_number" placeholder="03XX-XXXXXXX"/>  
        <label>Account Name</label><input id="jc_name" placeholder="Tumhara Naam"/>  
      </div>  
      <div>  
        <h3>Bank</h3>  
        <label>Bank Name</label><input id="bank_name" placeholder="HBL"/>  
        <label>Account Number</label><input id="bank_acc" placeholder="XXXXXXXXXXXXXXX"/>  
        <label>Account Holder Name</label><input id="bank_holder" placeholder="Tumhara Naam"/>  
        <label>IBAN</label><input id="bank_iban" placeholder="PK00XXXX..."/>  
      </div>  
    </div>  
    <div class="row" style="margin-top:10px"><button id="btnSavePayment">💾 Save Payment</button></div>  
  </div>  
  
  <div class="card">  
    <h2 style="margin:0 0 10px">🎨 Products</h2>  
    <div class="muted" style="margin-bottom:8px;font-size:13px">  
      Paste products array JSON. Each product: {id,name,price,description,features,downloadLink,active}  
    </div>  
    <textarea id="productsJson" style="min-height:220px"></textarea>  
    <div class="row" style="margin-top:10px"><button id="btnSaveProducts">💾 Save Products</button></div>  
  </div>  
  
  <div class="card">  
    <h2 style="margin:0 0 10px">📢 Broadcast (AI message)</h2>  
  
    <label>Offer Details for AI</label>  
    <textarea id="offerDetails" style="min-height:90px"></textarea>  
  
    <div class="row" style="margin-top:6px">  
      <div style="flex:1;min-width:220px">  
        <label>Message Type</label>  
        <select id="msgType">  
          <option value="personalized">Personalized (name)</option>  
          <option value="same">Same message</option>  
        </select>  
      </div>  
      <div style="flex:0 0 auto">  
        <button id="btnGenerate">🤖 Generate</button>  
      </div>  
    </div>  
  
    <div id="generatedWrap" style="display:none;margin-top:12px">  
      <label>Generated Message (editable)</label>  
      <textarea id="msgPreview" style="min-height:120px"></textarea>  
    </div>  
  
    <hr/>  
  
    <div class="row">  
      <div style="flex:1">  
        <label>Search contacts</label>  
        <input id="chatSearch" placeholder="🔍 Search..." />  
      </div>  
      <div style="flex:0 0 auto;align-self:flex-end">  
        <button class="gray" id="btnLoadChats">Load Chats</button>  
      </div>  
    </div>  
  
    <div class="chats" id="chatsList" style="margin-top:12px"></div>  
  
    <div class="row" style="margin-top:10px">  
      <div style="flex:0 0 220px">  
        <label>Delay between messages (sec)</label>  
        <input id="bc_delay" type="number" value="5" min="1" max="60"/>  
      </div>  
      <div style="flex:1">  
        <button id="btnBroadcast" style="width:100%">📨 Send Broadcast</button>  
      </div>  
    </div>  
  
  </div>  
</div>  
  
<div class="toast" id="toast"></div>  
  
<script>  
let allData = null;  
let allChats = [];  
let selectedChats = new Set();  
  
function $(id){ return document.getElementById(id); }  
function toast(msg){ const el = $('toast'); el.textContent = msg; el.style.display='block'; setTimeout(()=>el.style.display='none',2500); }  
  
async function api(path, opts){  
  const res = await fetch(path, opts);  
  const ct = (res.headers.get('content-type')||'');  
  if(ct.includes('application/json')) return res.json();  
  return res.text();  
}  
  
async function loadData(){  
  allData = await api('/api/data');  
  render();  
}  
  
function orderCard(o){  
  const hasShot = o.hasScreenshot ? '✅ Received' : '❌ Pending';  
  const time = o.timestamp ? new Date(o.timestamp).toLocaleString('en-PK') : '';  
  const badge = o.language ? '<span class="badge" style="border-color:#333;color:#aaa;background:#0f0f0f;margin-left:8px">'+o.language+'</span>' : '';  
  if(o.status === 'pending'){  
    return \`  
      <div class="order">  
        <div class="top">  
          <div>  
            <b>#\${o.orderId}</b>\${badge}  
            <div class="muted" style="font-size:12px;margin-top:4px">\${o.status}</div>  
          </div>  
          <div class="row" style="gap:8px;justify-content:flex-end">  
            <button class="blue" style="padding:8px 12px" onclick="approve(\${o.orderId})">✅ Approve</button>  
            <button class="red" style="padding:8px 12px" onclick="reject(\${o.orderId})">❌ Reject</button>  
          </div>  
        </div>  
        <div class="meta">  
          📱 \${o.customerNumber||''}  
          👤 \${o.customerName||''}  
          📸 \${hasShot}  
          🕒 \${time}  
        </div>  
      </div>  
    \`;  
  }  
  return \`  
    <div class="order">  
      <div class="top">  
        <div>  
          <b>#\${o.orderId}</b>\${badge}  
          <div class="muted" style="font-size:12px;margin-top:4px">\${o.status}</div>  
        </div>  
      </div>  
      <div class="meta">  
        📱 \${o.customerNumber||''}  
        👤 \${o.customerName||''}  
        📸 \${hasShot}  
        🕒 \${time}  
      </div>  
    </div>  
  \`;  
}  
  
function render(){  
  const s = allData.stats || {};  
  $('botBadge').className = 'badge ' + (allData.botStatus === 'connected' ? 'live' : 'off');  
  $('botBadge').textContent = allData.botStatus === 'connected' ? 'Bot Live' : allData.botStatus;  
  $('statsLine').textContent = \`Pending: \${s.pending||0} | Approved: \${s.approved||0} | Rejected: \${s.rejected||0}\`;  
  
  const orders = Object.values(allData.orders||{}).sort((a,b)=> (b.timestamp||0)-(a.timestamp||0));  
  const pending = orders.filter(o=>o.status==='pending');  
  const approved = orders.filter(o=>o.status==='approved');  
  const rejected = orders.filter(o=>o.status==='rejected');  
  
  $('pendingList').innerHTML = pending.length ? pending.map(orderCard).join('') : '<div class="muted">No pending orders</div>';  
  $('approvedList').innerHTML = approved.length ? approved.map(orderCard).join('') : '<div class="muted">No approved orders</div>';  
  $('rejectedList').innerHTML = rejected.length ? rejected.map(orderCard).join('') : '<div class="muted">No rejected orders</div>';  
  
  $('aiPrompt').value = allData.aiPrompt || '';  
  $('s_bizName').value = allData.settings?.businessName || '';  
  $('s_adminNum').value = allData.settings?.adminNumber || '';  
  
  $('ep_number').value = allData.payment?.easypaisa?.number || '';  
  $('ep_name').value = allData.payment?.easypaisa?.name || '';  
  $('jc_number').value = allData.payment?.jazzcash?.number || '';  
  $('jc_name').value = allData.payment?.jazzcash?.name || '';  
  
  $('bank_name').value = allData.payment?.bank?.bankName || '';  
  $('bank_acc').value = allData.payment?.bank?.accountNumber || '';  
  $('bank_holder').value = allData.payment?.bank?.accountName || '';  
  $('bank_iban').value = allData.payment?.bank?.iban || '';  
  
  $('productsJson').value = JSON.stringify(allData.products || [], null, 2);  
}  
  
async function approve(orderId){  
  if(!confirm('Approve order #'+orderId+'?')) return;  
  await api('/api/approve/'+orderId,{ method:'POST' });  
  toast('Approved ✅');  
  await loadData();  
}  
async function reject(orderId){  
  if(!confirm('Reject order #'+orderId+'?')) return;  
  await api('/api/reject/'+orderId,{ method:'POST' });  
  toast('Rejected ❌');  
  await loadData();  
}  
  
// Save handlers  
$('btnSavePrompt').onclick = async () => {  
  await api('/api/prompt',{  
    method:'POST',  
    headers:{'Content-Type':'application/json'},  
    body: JSON.stringify({ prompt: $('aiPrompt').value })  
  });  
  toast('Prompt saved ✅');  
};  
  
$('btnSaveSettings').onclick = async () => {  
  const pw = $('s_password').value;  
  const payload = {  
    businessName: $('s_bizName').value,  
    adminNumber: $('s_adminNum').value,  
    dashboardPassword: pw || allData.settings?.dashboardPassword  
  };  
  await api('/api/settings',{  
    method:'POST',  
    headers:{'Content-Type':'application/json'},  
    body: JSON.stringify(payload)  
  });  
  $('s_password').value='';  
  toast('Settings saved ✅');  
  await loadData();  
};  
  
$('btnSavePayment').onclick = async () => {  
  const payload = {  
    easypaisa:{ number:$('ep_number').value, name:$('ep_name').value },  
    jazzcash:{ number:$('jc_number').value, name:$('jc_name').value },  
    bank:{  
      bankName:$('bank_name').value,  
      accountNumber:$('bank_acc').value,  
      accountName:$('bank_holder').value,  
      iban:$('bank_iban').value  
    }  
  };  
  await api('/api/payment',{  
    method:'POST',  
    headers:{'Content-Type':'application/json'},  
    body: JSON.stringify(payload)  
  });  
  toast('Payment saved ✅');  
  await loadData();  
};  
  
$('btnSaveProducts').onclick = async () => {  
  let parsed;  
  try{  
    parsed = JSON.parse($('productsJson').value || '[]');  
    if(!Array.isArray(parsed)) throw new Error('products must be array');  
  }catch(e){  
    toast('Products JSON invalid: '+e.message);  
    return;  
  }  
  await api('/api/products',{  
    method:'POST',  
    headers:{'Content-Type':'application/json'},  
    body: JSON.stringify(parsed)  
  });  
  toast('Products saved ✅');  
  await loadData();  
};  
  
// Broadcast UI  
async function loadChats(){  
  const d = await api('/api/chats');  
  allChats = d.chats || [];  
  selectedChats = new Set();  
  renderChats();  
}  
  
function renderChats(){  
  const q = ($('chatSearch').value || '').toLowerCase();  
  const list = allChats.filter(c=>{  
    return (c.name||'').toLowerCase().includes(q) || (c.number||'').includes(q);  
  });  
  $('chatsList').innerHTML = list.map(c=>{  
    const checked = selectedChats.has(c.jid) ? 'checked' : '';  
    return \`  
      <div class="chatItem">  
        <label style="cursor:pointer">  
          <input type="checkbox" ${checked}  
            onchange="toggleChat('\${c.jid}')"  
          />  
          <b>\${c.name||c.number}</b>  
          <div class="muted" style="font-size:12px">\${c.number}</div>  
        </label>  
      </div>  
    \`;  
  }).join('') || '<div class="muted">No chats</div>';  
}  
  
function toggleChat(jid){  
  if(selectedChats.has(jid)) selectedChats.delete(jid);  
  else selectedChats.add(jid);  
  renderChats();  
}  
  
$('btnLoadChats').onclick = async () => {  
  if(allData?.botStatus !== 'connected'){  
    toast('Bot connect pehle karo.');  
    return;  
  }  
  await loadChats();  
};  
  
$('chatSearch').addEventListener('input', ()=>renderChats());  
  
$('btnGenerate').onclick = async () => {  
  const offerDetails = $('offerDetails').value || '';  
  if(!offerDetails.trim()){ toast('Offer details likho'); return; }  
  
  const personalized = $('msgType').value === 'personalized';  
  $('btnGenerate').disabled = true;  
  try{  
    const d = await api('/api/generate-message',{  
      method:'POST',  
      headers:{'Content-Type':'application/json'},  
      body: JSON.stringify({ offerDetails, customerName:'Dost', personalized })  
    });  
    if(d.success){  
      $('msgPreview').value = d.message || '';  
      $('generatedWrap').style.display='block';  
      toast('Message generated ✅');  
    }else{  
      toast('Generate failed');  
    }  
  }catch(e){  
    toast('Error: '+e.message);  
  }finally{  
    $('btnGenerate').disabled = false;  
  }  
};  
  
$('btnBroadcast').onclick = async () => {  
  if(allData?.botStatus !== 'connected'){ toast('Bot connect pehle karo'); return; }  
  
  const selected = allChats.filter(c=>selectedChats.has(c.jid)).map(c=>({  
    jid: c.jid,  
    name: c.name || c.number,  
    number: c.number  
  }));  
  
  if(selected.length === 0){ toast('Contacts select karo'); return; }  
  const offerDetails = $('offerDetails').value || '';  
  const baseMessage = $('msgPreview').value || '';  
  if(!baseMessage.trim() && !offerDetails.trim()){ toast('Generate message ya offer details do'); return; }  
  
  const personalized = $('msgType').value === 'personalized';  
  const delaySeconds = parseInt($('bc_delay').value || '5', 10) || 5;  
  
  if(!confirm('Send broadcast to '+selected.length+' contacts?')) return;  
  
  $('btnBroadcast').disabled = true;  
  
  try{  
    await api('/api/smart-broadcast',{  
      method:'POST',  
      headers:{'Content-Type':'application/json'},  
      body: JSON.stringify({  
        offerDetails,  
        baseMessage,  
        personalized,  
        delaySeconds,  
        selectedContacts: selected  
      })  
    });  
    toast('Broadcast started ✅');  
    await loadData();  
  }catch(e){  
    toast('Broadcast error: '+e.message);  
  }finally{  
    $('btnBroadcast').disabled = false;  
  }  
};  
  
// Init  
$('btnReload').onclick = () => loadData().then(loadChats).catch(()=>{});  
loadData().catch(e=>toast('Load error: '+e.message));  
  
// Poll every 15s (light)  
setInterval(()=>loadData().catch(()=>{}), 15000);  
</script>  
</body>  
</html>`;  
}  
  
// ─────────────────────────────────────────// Web server routes // ─────────────────────────────────────────  
const server = http.createServer(async (req, res) => {  
  const parsedUrl = url.parse(req.url, true);  
  const pathname = parsedUrl.pathname;  
  const method = req.method || 'GET';  
  
  // LOGIN  
  if (pathname === '/login') {  
    if (method === 'POST') {  
      const body = await parseBody(req);  
      if (body.password === botData.settings.dashboardPassword) {  
        const sessionId = Math.random().toString(36).substring(2);  
        sessions[sessionId] = true;  
        res.writeHead(200, {  
          'Set-Cookie': `session=${sessionId}; Path=/; HttpOnly`,  
          'Content-Type': 'application/json'  
        });  
        res.end(JSON.stringify({ success: true }));  
      } else {  
        res.writeHead(401, { 'Content-Type': 'application/json' });  
        res.end(JSON.stringify({ success: false, message: 'Wrong password!' }));  
      }  
      return;  
    }  
  
    res.writeHead(200, { 'Content-Type': 'text/html' });  
    res.end(`<!DOCTYPE html>  
<html>  
<head>  
<title>Login</title>  
<meta name="viewport" content="width=device-width,initial-scale=1"/>  
<style>  
*{margin:0;padding:0;box-sizing:border-box;}  
body{background:#0f0f0f;color:white;font-family:Segoe UI,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;}  
.box{background:#1a1a1a;padding:40px;border-radius:16px;width:90%;max-width:380px;border:1px solid #333;text-align:center;}  
h1{color:#25D366;font-size:24px;margin-bottom:8px;}  
p{color:#aaa;font-size:13px;margin-bottom:25px;}  
input{width:100%;padding:12px 15px;background:#0f0f0f;border:1px solid #333;border-radius:8px;color:white;font-size:15px;margin-bottom:15px;outline:none;}  
input:focus{border-color:#25D366;}  
button{width:100%;padding:12px;background:#25D366;border:none;border-radius:8px;color:black;font-size:16px;font-weight:bold;cursor:pointer;}  
button:hover{background:#1ebe57;}  
.err{color:#e74c3c;font-size:13px;margin-top:10px;display:none;}  
</style>  
</head>  
<body>  
<div class="box">  
  <h1>🏪 ${botData.settings.businessName}</h1>  
  <p>Admin Dashboard Login</p>  
  <input type="password" id="pass" placeholder="Password" onkeypress="if(event.key==='Enter')login()"/>  
  <button onclick="login()">🔐 Login</button>  
  <div class="err" id="err">❌ Wrong password!</div>  
</div>  
<script>  
async function login(){  
  const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pass').value})});  
  const d=await r.json();  
  if(d.success) window.location='/dashboard';  
  else document.getElementById('err').style.display='block';  
}  
</script>  
</body>  
</html>`);  
    return;  
  }  
  
  // AUTH CHECK (except /login and /qr)  
  if (pathname !== '/qr' && pathname !== '/login' && !isAuthenticated(req)) {  
    res.writeHead(302, { Location: '/login' });  
    res.end();  
    return;  
  }  
  
  // QR PAGE  
  if (pathname === '/qr') {  
    res.writeHead(200, { 'Content-Type': 'text/html' });  
  
    if (botStatus === 'connected') {  
      res.end(`<!doctype html>  
<html><head><style>  
body{background:#111;color:white;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif;text-align:center;}  
h2{color:#25D366;}a{color:#25D366;font-size:18px;margin-top:20px;text-decoration:none;}  
p{color:#aaa;}  
</style></head><body>  
<h2>✅ Bot Connected!</h2><p>Mega Agency Bot live hai!</p>  
<a href="/dashboard">📊 Dashboard Kholo</a>  
</body></html>`);  
      return;  
    }  
  
    if (!currentQR) {  
      res.end(`<!doctype html><html><head><meta http-equiv="refresh" content="3"/>  
<style>  
body{background:#111;color:white;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif;text-align:center;}  
h2{color:#f39c12;}p{color:#aaa;}  
</style></head><body>  
<h2>⏳ QR Generate Ho Raha Hai...</h2><p>Status: ${botStatus}</p></body></html>`);  
      return;  
    }  
  
    try {  
      const qrDataURL = await QRCode.toDataURL(currentQR, { width: 300, margin: 2 });  
      res.end(`<!doctype html><html><head>  
<meta http-equiv="refresh" content="25"/>  
<style>  
body{background:#111;color:white;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif;text-align:center;padding:20px;}  
img{border:8px solid white;border-radius:12px;width:280px;height:280px;}  
h2{color:#25D366;}  
p{color:#aaa;}  
</style></head><body>  
<h2>📱 WhatsApp QR Code</h2>  
<img src="${qrDataURL}"/>  
<p style="color:#f39c12;margin-top:15px">⚠️ 25 sec mein expire!</p>  
<p>Mega Agency bot live karne ke liye scan karo.</p>  
</body></html>`);  
    } catch (e) {  
      res.end(`<h1 style="color:red">QR Error: ${e.message}</h1>`);  
    }  
    return;  
  }  
  
  // DASHBOARD  
  if (pathname === '/dashboard' || pathname === '/') {  
    res.writeHead(200, { 'Content-Type': 'text/html' });  
    res.end(dashboardHtml());  
    return;  
  }  
  
  // API: GET DATA  
  if (pathname === '/api/data' && method === 'GET') {  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
  
    const ordersArr = Object.values(botData.orders || {});  
    const revenue = ordersArr  
      .filter((o) => o.status === 'approved')  
      .reduce((sum, o) => {  
        const pr = botData.products.find((p) => p.id === o.productId) || botData.products[0];  
        return sum + (pr?.price || 0);  
      }, 0);  
  
    res.end(  
      JSON.stringify({  
        ...botData,  
        botStatus,  
        chatsLoaded,  
        stats: {  
          pending: ordersArr.filter((o) => o.status === 'pending').length,  
          approved: ordersArr.filter((o) => o.status === 'approved').length,  
          rejected: ordersArr.filter((o) => o.status === 'rejected').length,  
          total: ordersArr.length,  
          customers: Object.keys(botData.customers || {}).length,  
          existingChats: existingChats.length,  
          revenue  
        }  
      })  
    );  
    return;  
  }  
  
  // API: GET CHATS  
  if (pathname === '/api/chats' && method === 'GET') {  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
    res.end(  
      JSON.stringify({  
        chats: existingChats,  
        loaded: chatsLoaded,  
        count: existingChats.length  
      })  
    );  
    return;  
  }  
  
  // API: GENERATE MESSAGE  
  if (pathname === '/api/generate-message' && method === 'POST') {  
    const body = await parseBody(req);  
    try {  
      const msg = await generateBroadcastMessage(  
        body.offerDetails || '',  
        body.customerName || 'Dost',  
        body.personalized || false  
      );  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: true, message: msg }));  
    } catch (e) {  
      res.writeHead(500, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: false, error: e.message }));  
    }  
    return;  
  }  
  
  // API: SMART BROADCAST  
  if (pathname === '/api/smart-broadcast' && method === 'POST') {  
    const body = await parseBody(req);  
  
    if (!body.selectedContacts || body.selectedContacts.length === 0) {  
      res.writeHead(400, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: false, error: 'Contacts select karo!' }));  
      return;  
    }  
  
    const bc = {  
      id: Date.now(),  
      offerDetails: body.offerDetails || '',  
      baseMessage: body.baseMessage || '',  
      personalized: body.personalized || false,  
      delaySeconds: body.delaySeconds || 5,  
      selectedContacts: body.selectedContacts,  
      status: 'pending',  
      sentCount: 0,  
      failedCount: 0,  
      totalContacts: body.selectedContacts.length,  
      createdAt: Date.now()  
    };  
  
    botData.broadcasts = botData.broadcasts || [];  
    botData.broadcasts.unshift(bc);  
    if (botData.broadcasts.length > 20) botData.broadcasts = botData.broadcasts.slice(0, 20);  
  
    await saveData();  
  
    if (!broadcastRunning) runBroadcast(bc).catch(console.error);  
  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
    res.end(JSON.stringify({ success: true, broadcast: bc }));  
    return;  
  }  
  
  // API: SETTINGS  
  if (pathname === '/api/settings' && method === 'POST') {  
    const b = await parseBody(req);  
    botData.settings = { ...botData.settings, ...b };  
    await saveData();  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
    res.end(JSON.stringify({ success: true }));  
    return;  
  }  
  
  // API: PAYMENT  
  if (pathname === '/api/payment' && method === 'POST') {  
    const b = await parseBody(req);  
    botData.payment = b;  
    await saveData();  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
    res.end(JSON.stringify({ success: true }));  
    return;  
  }  
  
  // API: PRODUCTS  
  if (pathname === '/api/products' && method === 'POST') {  
    const b = await parseBody(req);  
    botData.products = b;  
    await saveData();  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
    res.end(JSON.stringify({ success: true }));  
    return;  
  }  
  
  // API: PROMPT  
  if (pathname === '/api/prompt' && method === 'POST') {  
    const b = await parseBody(req);  
    botData.aiPrompt = b.prompt;  
    await saveData();  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
    res.end(JSON.stringify({ success: true }));  
    return;  
  }  
  
  // API: APPROVE ORDER  
  if (pathname.startsWith('/api/approve/') && method === 'POST') {  
    const orderId = parseInt(pathname.split('/api/approve/')[1], 10);  
    const order = Object.values(botData.orders || {}).find((o) => o.orderId === orderId);  
  
    if (order && sockGlobal) {  
      order.status = 'approved';  
      await saveData();  
  
      const product =  
        botData.products.find((p) => p.id === order.productId) || botData.products[0];  
  
      try {  
        const aiMsg = await getPaymentApprovedMessageAI({  
          userId: order.customerJid,  
          customerName: order.customerName || 'Customer',  
          lang: order.language || 'roman_urdu',  
          order,  
          product  
        });  
  
        await sockGlobal.sendMessage(order.customerJid, { text: aiMsg });  
  
        await saveToSheet({  
          ...order,  
          product: product.name,  
          amount: product.price,  
          status: 'approved'  
        });  
      } catch (e) {  
        console.log('Approve err:', e.message);  
      }  
    }  
  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
    res.end(JSON.stringify({ success: true }));  
    return;  
  }  
  
  // API: REJECT ORDER  
  if (pathname.startsWith('/api/reject/') && method === 'POST') {  
    const orderId = parseInt(pathname.split('/api/reject/')[1], 10);  
    const order = Object.values(botData.orders || {}).find((o) => o.orderId === orderId);  
  
    if (order && sockGlobal) {  
      order.status = 'rejected';  
      await saveData();  
  
      try {  
        const aiMsg = await getPaymentRejectedMessageAI({  
          userId: order.customerJid,  
          customerName: order.customerName || 'Customer',  
          lang: order.language || 'roman_urdu',  
          order  
        });  
  
        await sockGlobal.sendMessage(order.customerJid, { text: aiMsg });  
  
        await saveToSheet({  
          ...order,  
          status: 'rejected'  
        });  
      } catch {  
        // ignore  
      }  
    }  
  
    res.writeHead(200, { 'Content-Type': 'application/json' });  
    res.end(JSON.stringify({ success: true }));  
    return;  
  }  
  
  // API: SEND CUSTOM MESSAGE  
  if (pathname === '/api/send-message' && method === 'POST') {  
    const b = await parseBody(req);  
    if (sockGlobal && b.jid && b.message) {  
      try {  
        await sockGlobal.sendMessage(b.jid, { text: b.message });  
        res.writeHead(200, { 'Content-Type': 'application/json' });  
        res.end(JSON.stringify({ success: true }));  
      } catch (e) {  
        res.writeHead(500, { 'Content-Type': 'application/json' });  
        res.end(JSON.stringify({ success: false, error: e.message }));  
      }  
    } else {  
      res.writeHead(400, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: false }));  
    }  
    return;  
  }  
  
  // LOGOUT  
  if (pathname === '/logout') {  
    res.writeHead(302, {  
      'Set-Cookie': 'session=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT',  
      Location: '/login'  
    });  
    res.end();  
    return;  
  }  
  
  res.writeHead(404, { 'Content-Type': 'application/json' });  
  res.end(JSON.stringify({ error: 'Not found' }));  
});  
  
// ─────────────────────────────────────────// MAIN // ─────────────────────────────────────────  
(async () => {  
  await loadData();  
  console.log('🚀 Mega Agency AI Sales Bot v2 (Upstash WA Auth + AI Replies) STARTING...');  
  server.listen(process.env.PORT || 3000, () => {  
    console.log(' Server ready! /dashboard | /qr');  
  });  
  startBot();  
})();
