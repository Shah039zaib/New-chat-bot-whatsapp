require('dotenv').config();  
  
const fs = require('fs');  
const path = require('path');  
const zlib = require('zlib');  
const crypto = require('crypto');  
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
  downloadMediaMessage  
} = require('@whiskeysockets/baileys');  
  
// Global safety logs for Render  
process.on('unhandledRejection', (reason) => console.error('❗ unhandledRejection:', reason));  
process.on('uncaughtException', (err) => console.error('❗ uncaughtException:', err));  
  
/* ─────────────────────────────  
   ENV / CONFIG  
───────────────────────────── */  
const PORT = process.env.PORT || 3000;  
  
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;  
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;  
const upstashEnabled = !!(REDIS_URL && REDIS_TOKEN);  
  
const BOT_DATA_KEY = process.env.UPSTASH_BOT_DATA_KEY || 'bot_data_v7';  
const BOT_DATA_FILE = process.env.BOT_DATA_FILE || '/tmp/bot_data_v7.json';  
  
const WA_AUTH_DIR = process.env.WHATSAPP_AUTH_DIR || '/tmp/auth_info';  
const WA_AUTH_PREFIX = process.env.WHATSAPP_UPSTASH_AUTH_PREFIX || 'mega-agency-bot:wa-auth:v1';  
const WA_AUTH_CREDS_KEY = `${WA_AUTH_PREFIX}:creds`;  
const WA_AUTH_SNAPSHOT_KEY = `${WA_AUTH_PREFIX}:snapshot`;  
  
const WHATSAPP_UPSTASH_SAVE_SNAPSHOT =  
  (process.env.WHATSAPP_UPSTASH_SAVE_SNAPSHOT || 'true').toLowerCase() === 'true';  
  
const WHATSAPP_UPSTASH_SNAPSHOT_MAX_BYTES = parseInt(  
  process.env.WHATSAPP_UPSTASH_SNAPSHOT_MAX_BYTES || '900000',  
  10  
);  
  
const WHATSAPP_UPSTASH_SNAPSHOT_INTERVAL_MS = parseInt(  
  process.env.WHATSAPP_UPSTASH_SNAPSHOT_INTERVAL_MS || '60000',  
  10  
);  
  
const WHATSAPP_UPSTASH_CLEAR_ON_LOGGED_OUT =  
  (process.env.WHATSAPP_UPSTASH_CLEAR_ON_LOGGED_OUT || 'true').toLowerCase() === 'true';  
  
const GROQ_API_KEY = process.env.GROQ_API_KEY || '';  
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY || '';  
const MAX_AI_CONCURRENCY = parseInt(process.env.MAX_AI_CONCURRENCY || '3', 10);  
  
const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL || '';  
const GOOGLE_PRIVATE_KEY = (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n');  
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID || '';  
  
/* ─────────────────────────────  
   SIMPLE SEMAPHORE FOR AI  
───────────────────────────── */  
class Semaphore {  
  constructor(max) {  
    this.max = max;  
    this.current = 0;  
    this.queue = [];  
  }  
  async acquire() {  
    if (this.current < this.max) {  
      this.current++;  
      return;  
    }  
    return new Promise((resolve) => this.queue.push(resolve));  
  }  
  release() {  
    this.current--;  
    if (this.queue.length) {  
      this.current++;  
      const next = this.queue.shift();  
      next();  
    }  
  }  
}  
  
const aiSemaphore = new Semaphore(Math.max(1, MAX_AI_CONCURRENCY));  
async function withAiLock(fn) {  
  await aiSemaphore.acquire();  
  try {  
    return await fn();  
  } finally {  
    aiSemaphore.release();  
  }  
}  
  
/* ─────────────────────────────  
   UPSTASH REDIS REST  
───────────────────────────── */  
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
      { headers: { Authorization: `Bearer ${REDIS_TOKEN}` }, timeout: 8000 }  
    );  
    return true;  
  } catch {  
    return false;  
  }  
}  
  
/* ─────────────────────────────  
   BOT DATA DEFAULT + LOAD/SAVE  
───────────────────────────── */  
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
        features: ['100+ Premium Themes', 'All Niches Covered', 'Regular Updates', '24/7 Support'],  
        downloadLink: '',  
        active: true  
      }  
    ],  
    aiPrompt: `Tum Mega Agency ke professional AI Sales Agent ho. Tumhara naam "Max" hai.  
  
SERVICE:  
- Product: 100+ Premium Shopify Themes Mega Bundle  
- FINAL PRICE: PKR 999 ONLY (discount bilkul nahi)  
  
LANGUAGE:  
Customer ki language follow karo (Urdu / Roman Urdu / English)  
  
RULES:  
- Reply short: 3-4 lines max  
- Friendly emojis use karo  
- Price negotiation: Discount KABHI NAHI — PKR 999 final  
- Jab customer BUY karna chahe: reply bilkul START mein "ORDER_READY" word se start karo  
- ORDER_READY ke baad normal message do  
  
SELLING:  
- Market mein ek theme 5000+ ki hoti hai; 100+ sirf PKR 999  
- Sirf PKR 10 per theme  
- Competitors already use kar rahe hain (FOMO)  
  
IMPORTANT:  
- ORDER_READY sirf buy time par.  
- Koi aur price mention na karna.`,  
    orders: {},  
    orderCounter: 1000  
  };  
}  
  
let botData = getDefaultData();  
  
let saveInFlight = false;  
let saveQueued = false;  
let saveTimer = null;  
  
function requestSaveData(delayMs = 200) {  
  if (saveTimer) clearTimeout(saveTimer);  
  saveTimer = setTimeout(() => {  
    saveTimer = null;  
    runSaveData().catch(() => {});  
  }, delayMs);  
}  
  
async function loadData() {  
  try {  
    if (upstashEnabled) {  
      const saved = await redisGet(BOT_DATA_KEY);  
      if (saved && typeof saved === 'object') {  
        botData = { ...getDefaultData(), ...saved };  
        botData.orders = botData.orders || {};  
        botData.products = Array.isArray(botData.products) ? botData.products : getDefaultData().products;  
        return;  
      }  
    }  
    if (fs.existsSync(BOT_DATA_FILE)) {  
      const saved2 = JSON.parse(fs.readFileSync(BOT_DATA_FILE, 'utf8'));  
      botData = { ...getDefaultData(), ...saved2 };  
      botData.orders = botData.orders || {};  
      botData.products = Array.isArray(botData.products) ? botData.products : getDefaultData().products;  
    }  
  } catch (e) {  
    console.log('Load error:', e.message);  
  }  
}  
  
async function runSaveData() {  
  if (saveInFlight) {  
    saveQueued = true;  
    return;  
  }  
  saveInFlight = true;  
  saveQueued = false;  
  
  try {  
    if (upstashEnabled) await redisSet(BOT_DATA_KEY, botData);  
    fs.writeFileSync(BOT_DATA_FILE, JSON.stringify(botData, null, 2));  
  } catch (e) {  
    console.log('Save error:', e.message);  
  } finally {  
    saveInFlight = false;  
    if (saveQueued) runSaveData().catch(() => {});  
  }  
}  
  
/* ─────────────────────────────  
   LANGUAGE DETECTION  
───────────────────────────── */  
function detectLang(text) {  
  if (!text) return 'roman_urdu';  
  if (/[\u0600-\u06FF]/.test(text)) return 'urdu';  
  if (  
    /\b(kya|hai|haan|nahi|bhai|yar|chahiye|theek|acha|karo|dedo|batao|kitna|lena|mujhe|yrr)\b/i.test(  
      text  
    )  
  )  
    return 'roman_urdu';  
  return 'english';  
}  
  
/* ─────────────────────────────  
   UTIL: DIGITS + VALIDATORS  
───────────────────────────── */  
function digitsOnly(str) {  
  return (str || '').toString().replace(/\D/g, '');  
}  
  
/* ─────────────────────────────  
   GOOGLE SHEETS (OPTIONAL)  
───────────────────────────── */  
async function getGoogleToken() {  
  try {  
    if (!GOOGLE_CLIENT_EMAIL || !GOOGLE_PRIVATE_KEY || !GOOGLE_SHEET_ID) return null;  
  
    const now = Math.floor(Date.now() / 1000);  
  
    const header = Buffer.from(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).toString('base64url');  
    const payload = Buffer.from(  
      JSON.stringify({  
        iss: GOOGLE_CLIENT_EMAIL,  
        scope: 'https://www.googleapis.com/auth/spreadsheets',  
        aud: 'https://oauth2.googleapis.com/token',  
        exp: now + 3600,  
        iat: now  
      })  
    ).toString('base64url');  
  
    const sign = crypto.createSign('RSA-SHA256');  
    sign.update(`${header}.${payload}`);  
  
    const jwt = `${header}.${payload}.${sign.sign(GOOGLE_PRIVATE_KEY, 'base64url')}`;  
  
    const res = await axios.post('https://oauth2.googleapis.com/token', {  
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',  
      assertion: jwt  
    });  
  
    return res.data?.access_token || null;  
  } catch {  
    return null;  
  }  
}  
  
async function initSheet() {  
  try {  
    const token = await getGoogleToken();  
    if (!token) return;  
  
    const sheetId = GOOGLE_SHEET_ID;  
  
    await axios.post(  
      `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/Sheet1!A1:append?valueInputOption=USER_ENTERED`,  
      {  
        values: [['Order ID', 'Customer', 'Phone', 'Product', 'Amount', 'Status', 'Language', 'Date']]  
      },  
      { headers: { Authorization: `Bearer ${token}` } }  
    );  
  } catch {  
    // ignore  
  }  
}  
  
async function saveToSheet(data) {  
  try {  
    const token = await getGoogleToken();  
    if (!token) return;  
  
    const sheetId = GOOGLE_SHEET_ID;  
  
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
            new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' })  
          ]  
        ]  
      },  
      { headers: { Authorization: `Bearer ${token}` } }  
    );  
  } catch {  
    // ignore  
  }  
}  
  
/* ─────────────────────────────  
   VOICE -> TEXT (Groq Whisper)  
───────────────────────────── */  
async function voiceToText(audioBuffer) {  
  try {  
    const FormData = require('form-data');  
    const form = new FormData();  
    form.append('file', audioBuffer, { filename: 'audio.ogg', contentType: 'audio/ogg' });  
    form.append('model', 'whisper-large-v3');  
    form.append('response_format', 'json');  
  
    if (!GROQ_API_KEY) return null;  
  
    const res = await axios.post('https://api.groq.com/openai/v1/audio/transcriptions', form, {  
      headers: { ...form.getHeaders(), Authorization: `Bearer ${GROQ_API_KEY}` },  
      timeout: 30000  
    });  
  
    return res.data?.text || null;  
  } catch {  
    return null;  
  }  
}  
  
/* ─────────────────────────────  
   LLM CALL  
───────────────────────────── */  
const AI_MODELS = [  
  { provider: 'groq', model: 'llama-3.3-70b-versatile' },  
  { provider: 'groq', model: 'llama-3.1-8b-instant' },  
  { provider: 'groq', model: 'gemma2-9b-it' },  
  { provider: 'openrouter', model: 'meta-llama/llama-3.1-8b-instruct:free' },  
  { provider: 'openrouter', model: 'google/gemma-2-9b-it:free' },  
  { provider: 'openrouter', model: 'mistralai/mistral-7b-instruct:free' }  
];  
  
async function callLLMChatCompletions(messages, { max_tokens = 300, temperature = 0.85 } = {}) {  
  return withAiLock(async () => {  
    for (const { provider, model } of AI_MODELS) {  
      try {  
        if (provider === 'groq' && !GROQ_API_KEY) continue;  
        if (provider === 'openrouter' && !OPENROUTER_API_KEY) continue;  
  
        const apiUrl =  
          provider === 'groq'  
            ? 'https://api.groq.com/openai/v1/chat/completions'  
            : 'https://openrouter.ai/api/v1/chat/completions';  
  
        const headers =  
          provider === 'groq'  
            ? { Authorization: `Bearer ${GROQ_API_KEY}`, 'Content-Type': 'application/json' }  
            : {  
                Authorization: `Bearer ${OPENROUTER_API_KEY}`,  
                'Content-Type': 'application/json',  
                'HTTP-Referer': 'https://mega-agency.com',  
                'X-Title': 'Mega Agency'  
              };  
  
        const response = await axios.post(  
          apiUrl,  
          { model, messages, max_tokens, temperature },  
          { headers, timeout: 20000 }  
        );  
  
        const content = response.data?.choices?.[0]?.message?.content;  
        if (content && content.trim()) return content.trim();  
      } catch {  
        // try next model  
      }  
    }  
    return null;  
  });  
}  
  
/* ─────────────────────────────  
   AI SALES RESPONSE  
───────────────────────────── */  
const salesHistory = {}; // per customerJid  
  
function getActiveProduct() {  
  return (botData.products || []).find((p) => p.active) || (botData.products || [])[0];  
}  
  
function langRule(lang) {  
  if (lang === 'urdu') return 'Sirf Urdu script mein reply karo.';  
  if (lang === 'roman_urdu') return 'Roman Urdu mein reply karo.';  
  return 'English mein reply karo.';  
}  
  
async function getAISalesResponse(userMessage, userId, customerName, lang) {  
  if (!salesHistory[userId]) salesHistory[userId] = [];  
  salesHistory[userId].push({ role: 'user', content: userMessage });  
  if (salesHistory[userId].length > 30) salesHistory[userId] = salesHistory[userId].slice(-30);  
  
  const activeProduct = getActiveProduct();  
  
  const systemPrompt =  
    (botData.aiPrompt || '') +  
    `\n\n${langRule(lang)}` +  
    `\nCustomer naam: ${customerName}` +  
    `\nActive Product: ${activeProduct?.name || ''}` +  
    `\nPrice: ${botData.settings.currency} ${activeProduct?.price || ''}` +  
    `\nYAD RAKHO: Price kabhi kam nahi karo!`;  
  
  try {  
    const aiText = await callLLMChatCompletions(  
      [{ role: 'system', content: systemPrompt }, ...salesHistory[userId]],  
      { max_tokens: 320, temperature: 0.85 }  
    );  
  
    if (!aiText) throw new Error('LLM returned empty');  
  
    salesHistory[userId].push({ role: 'assistant', content: aiText });  
    const shouldOrder = aiText.toUpperCase().includes('ORDER_READY');  
    const cleanMessage = aiText.replace(/ORDER_READY/gi, '').trim();  
  
    return { message: cleanMessage, shouldOrder, product: activeProduct };  
  } catch {  
    const fb = {  
      urdu: '⚠️ Thodi technical difficulty. 1 minute mein dobara try karo 🙏',  
      roman_urdu: '⚠️ Thodi technical difficulty. 1 minute mein dobara try karo 🙏',  
      english: '⚠️ Technical difficulty. Try again in 1 minute 🙏'  
    };  
    return { message: fb[lang] || fb.roman_urdu, shouldOrder: false, product: activeProduct };  
  }  
}  
  
/* ─────────────────────────────  
   AI SCENARIO MESSAGES (NO ORDER_READY)  
───────────────────────────── */  
async function getAIScenarioMessage({  
  userId,  
  customerName,  
  lang,  
  scenarioName,  
  scenarioDetails,  
  fallbackText  
}) {  
  const activeProduct = getActiveProduct();  
  
  const systemPrompt =  
    (botData.aiPrompt || '') +  
    `\n\n${langRule(lang)}` +  
    `\nCustomer naam: ${customerName}` +  
    `\nActive Product: ${activeProduct?.name || ''}` +  
    `\nPrice: ${botData.settings.currency} ${activeProduct?.price || ''}` +  
    `\n\nSCENARIO: ${scenarioName}\n${scenarioDetails}` +  
    `\nIMPORTANT: ORDER_READY word bilkul mat likhna. Agar aa jaye to hata do.` +  
    `\nReply short (3-5 lines). Emojis allowed.`;  
  
  const userPrompt = `SCENARIO=${scenarioName}\nLANG=${lang}\nDETAILS:\n${scenarioDetails}`;  
  
  try {  
    const aiText = await callLLMChatCompletions(  
      [{ role: 'system', content: systemPrompt }, { role: 'user', content: userPrompt }],  
      { max_tokens: 240, temperature: 0.55 }  
    );  
  
    if (!aiText) throw new Error('empty scenario');  
  
    const cleaned = aiText.replace(/ORDER_READY/gi, '').trim();  
    return cleaned || fallbackText;  
  } catch {  
    return fallbackText;  
  }  
}  
  
/* ─────────────────────────────  
   STATIC TEMPLATES + FALLBACKS  
───────────────────────────── */  
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
  
  if (lang === 'urdu')  
    return `🛒 *آرڈر کنفرم! #${orderId}*\n\n${details}\n\n✅ پیمنٹ کے بعد اسکرین شاٹ بھیجیں\n⏳ 1 گھنٹے میں ڈلیوری!`;  
  if (lang === 'roman_urdu')  
    return `🛒 *Order Confirmed! #${orderId}*\nProduct: *${product.name}*\n\n${details}\n\n✅ Payment ke baad *screenshot* bhejo\n📦 Delivery 1 hour mein guaranteed!`;  
  return `🛒 *Order Confirmed! #${orderId}*\nProduct: *${product.name}*\n\n${details}\n\n✅ Send screenshot after payment\n📦 Delivery within 1 hour!`;  
}  
  
function getScreenshotReceivedStatic(orderId, lang) {  
  const msgs = {  
    urdu:  
      `📸 *اسکرین شاٹ موصول!*\n\nآرڈر *#${orderId}*\n✅ Admin verify kar raha hai\n⏳ Delivery 1 hour mein! 🙏`,  
    roman_urdu:  
      `📸 *Screenshot Receive Ho Gaya!*\n\nOrder *#${orderId}*\n✅ Admin verify kar raha hai\n⏳ 1 hour mein delivery! 🙏`,  
    english:  
      `📸 *Screenshot Received!*\n\nOrder *#${orderId}*\n✅ Admin is verifying\n⏳ Delivery within 1 hour! 🙏`  
  };  
  return msgs[lang] || msgs.roman_urdu;  
}  
  
function getPaymentApprovedStatic(order, product, lang) {  
  const downloadBlock = product?.downloadLink ? `\n\n⬇️ *Download Link:*\n${product.downloadLink}\n` : '';  
  const businessName = botData.settings.businessName || 'Mega Agency';  
  
  if (lang === 'urdu')  
    return `🎉 *Payment Approved!*\n\nOrder *#${order.orderId}* confirm ho gaya!\n\n📦 *${product.name}*${downloadBlock}\n\nمدد چاہیے تو میسج کریں!\nشکریہ ${businessName} کو choose کرنے کا! 🙏`;  
  
  if (lang === 'roman_urdu')  
    return `🎉 *Payment Approved!*\n\nOrder *#${order.orderId}* confirm ho gaya!\n\n📦 *${product.name}*${downloadBlock}\n\nKoi help chahiye? toh message karo!\nShukriya ${businessName} ko choose karne ka! 🙏`;  
  
  return `🎉 *Payment Approved!*\n\nOrder *#${order.orderId}* confirmed!\n\n📦 *${product.name}*${downloadBlock}\n\nAny help? Message us!\nThanks for choosing ${businessName}! 🙏`;  
}  
  
function getPaymentRejectedStatic(order, lang) {  
  if (lang === 'urdu')  
    return `❌ *پیمنٹ ویریفائی نہیں ہو سکی*\n\nOrder *#${order.orderId}*\n\nScreenshot clear nahi tha.\nبراہِ کرم دوبارہ sahi screenshot bhejیں ya admin se contact کریں.\n\n"buy" لکھ کر try karo 💪`;  
  
  if (lang === 'roman_urdu')  
    return `❌ *Payment Verify Nahi Ho Saki*\n\nOrder *#${order.orderId}*\n\nScreenshot sahi nahi tha.\nDobara sahi screenshot bhejo ya admin se contact karo.\n\n"buy" likhkar try karo 💪`;  
  
  return `❌ *Payment Verify Failed*\n\nOrder *#${order.orderId}*\n\nScreenshot wasn't clear.\nPlease resend a correct screenshot or contact admin.\n\nType "buy" to try again 💪`;  
}  
  
/* ─────────────────────────────  
   UPSTASH: WHATSAPP AUTH PERSISTENCE  
───────────────────────────── */  
function ensureDirSync(dir) {  
  fs.mkdirSync(dir, { recursive: true });  
}  
  
function snapshotAuthDir(authDir) {  
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
  
  return files;  
}  
  
async function restoreWhatsAppAuthFromUpstash() {  
  if (!upstashEnabled) return;  
  
  try {  
    ensureDirSync(WA_AUTH_DIR);  
  
    const snap = await redisGet(WA_AUTH_SNAPSHOT_KEY);  
    if (snap?.data) {  
      const gz = Buffer.from(snap.data, 'base64');  
      const jsonBuf = zlib.gunzipSync(gz);  
      const parsed = JSON.parse(jsonBuf.toString('utf8'));  
  
      if (parsed?.files && typeof parsed.files === 'object') {  
        fs.rmSync(WA_AUTH_DIR, { recursive: true, force: true });  
        ensureDirSync(WA_AUTH_DIR);  
  
        for (const [relPath, contentB64] of Object.entries(parsed.files)) {  
          const fullPath = path.join(WA_AUTH_DIR, relPath);  
          ensureDirSync(path.dirname(fullPath));  
          fs.writeFileSync(fullPath, Buffer.from(contentB64, 'base64'));  
        }  
        console.log('✅ WhatsApp auth restored from Upstash snapshot');  
        return;  
      }  
    }  
  } catch (e) {  
    console.log('Auth snapshot restore failed:', e.message);  
  }  
  
  try {  
    const creds = await redisGet(WA_AUTH_CREDS_KEY);  
    if (creds) {  
      ensureDirSync(WA_AUTH_DIR);  
      const credsPath = path.join(WA_AUTH_DIR, 'creds.json');  
      fs.writeFileSync(credsPath, JSON.stringify(creds, null, 2));  
      console.log('✅ WhatsApp creds.json restored from Upstash');  
    }  
  } catch {  
    // ignore  
  }  
}  
  
let authPersistInProgress = false;  
let authPersistQueued = false;  
let lastCredsPersistAt = 0;  
let lastSnapshotAt = 0;  
  
async function persistWhatsAppAuthToUpstash() {  
  if (!upstashEnabled) return;  
  
  if (authPersistInProgress) {  
    authPersistQueued = true;  
    return;  
  }  
  authPersistInProgress = true;  
  authPersistQueued = false;  
  
  try {  
    const credsPath = path.join(WA_AUTH_DIR, 'creds.json');  
    if (!fs.existsSync(credsPath)) return;  
  
    const now = Date.now();  
    const credsThrottleMs = parseInt(process.env.WHATSAPP_UPSTASH_CREDS_THROTTLE_MS || '5000', 10);  
    const shouldPersistCreds = !lastCredsPersistAt || now - lastCredsPersistAt >= credsThrottleMs;  
  
    if (shouldPersistCreds) {  
      const creds = JSON.parse(fs.readFileSync(credsPath, 'utf8'));  
      await redisSet(WA_AUTH_CREDS_KEY, creds);  
      lastCredsPersistAt = now;  
    }  
  
    const shouldSnapshot =  
      WHATSAPP_UPSTASH_SAVE_SNAPSHOT &&  
      now - lastSnapshotAt >= WHATSAPP_UPSTASH_SNAPSHOT_INTERVAL_MS;  
  
    if (shouldSnapshot) {  
      const files = snapshotAuthDir(WA_AUTH_DIR);  
      const payload = JSON.stringify({ v: 1, files });  
  
      const gz = zlib.gzipSync(Buffer.from(payload, 'utf8'), { level: 9 });  
      if (gz.length <= WHATSAPP_UPSTASH_SNAPSHOT_MAX_BYTES) {  
        await redisSet(WA_AUTH_SNAPSHOT_KEY, { v: 1, data: gz.toString('base64'), t: Date.now() });  
        lastSnapshotAt = now;  
      } else {  
        // too big; keep only creds.json  
      }  
    }  
  } catch (e) {  
    console.log('Auth persist error:', e.message);  
  } finally {  
    authPersistInProgress = false;  
    if (authPersistQueued) persistWhatsAppAuthToUpstash().catch(() => {});  
  }  
}  
  
async function clearWhatsAppAuthInUpstash() {  
  if (!upstashEnabled) return;  
  await redisDel(WA_AUTH_CREDS_KEY);  
  await redisDel(WA_AUTH_SNAPSHOT_KEY);  
}  
  
/* ─────────────────────────────  
   WHATSAPP BOT  
───────────────────────────── */  
let currentQR = null;  
let botStatus = 'starting';  
let sockGlobal = null;  
  
const perJidQueue = new Map();  
function enqueueForJid(jid, task) {  
  const prev = perJidQueue.get(jid) || Promise.resolve();  
  const next = prev  
    .then(task)  
    .catch((e) => console.error('Task error for', jid, e?.message || e))  
    .finally(() => {  
      if (perJidQueue.get(jid) === next) perJidQueue.delete(jid);  
    });  
  perJidQueue.set(jid, next);  
  return next;  
}  
  
function getAdminJid() {  
  const n = botData?.settings?.adminNumber;  
  if (!n) return null;  
  // Accept either "923..." or already with @s.whatsapp.net  
  if (n.includes('@s.whatsapp.net')) return n;  
  return n + '@s.whatsapp.net';  
}  
  
async function handleTextMessage(sock, message, senderId, senderName) {  
  const userMessage =  
    message.message?.conversation || message.message?.extendedTextMessage?.text || '';  
  
  if (!userMessage || !userMessage.trim()) return;  
  
  const lang = detectLang(userMessage);  
  
  await sock.sendPresenceUpdate('composing', senderId);  
  const aiReply = await getAISalesResponse(userMessage, senderId, senderName, lang);  
  await sock.sendPresenceUpdate('paused', senderId);  
  
  if (aiReply.message) {  
    await sock.sendMessage(senderId, { text: aiReply.message }, { quoted: message }).catch(() => {});  
  }  
  
  if (aiReply.shouldOrder) {  
    const activeProduct = aiReply.product || getActiveProduct();  
  
    // If pending order already exists, reuse it  
    const existingPending = botData.orders[senderId];  
    let order;  
  
    if (existingPending && existingPending.status === 'pending') {  
      order = existingPending;  
      order.language = lang;  
    } else {  
      botData.orderCounter++;  
      order = {  
        orderId: botData.orderCounter,  
        customerJid: senderId,  
        customerNumber: senderId.replace('@s.whatsapp.net', ''),  
        customerName: senderName,  
        productId: activeProduct?.id,  
        language: lang,  
        status: 'pending',  
        hasScreenshot: false,  
        timestamp: Date.now()  
      };  
      botData.orders[senderId] = order;  
    }  
  
    requestSaveData();  
  
    const productForOrder =  
      (botData.products || []).find((p) => p.id === order.productId) || activeProduct || (botData.products || [])[0];  
  
    // Save pending to sheet (optional)  
    saveToSheet({  
      orderId: order.orderId,  
      customerName: order.customerName,  
      customerNumber: order.customerNumber,  
      product: productForOrder?.name || '',  
      amount: productForOrder?.price || '',  
      status: 'pending',  
      language: order.language  
    }).catch(() => {});  
  
    await new Promise((r) => setTimeout(r, 700));  
    const payMsg = getPaymentMessage(order.orderId, productForOrder, lang);  
    await sock.sendMessage(senderId, { text: payMsg });  
  }  
}  
  
async function handleVoiceMessage(sock, message, senderId, senderName) {  
  await sock.sendPresenceUpdate('composing', senderId);  
  
  try {  
    const buf = await downloadMediaMessage(message, 'buffer', {});  
    const text = await voiceToText(buf);  
  
    if (!text || !text.trim()) {  
      const fallback = '🎤 Voice samajh nahi aayi. Please text mein likhein ya "buy" bhej dein 🙏';  
      await sock.sendPresenceUpdate('paused', senderId);  
      await sock.sendMessage(senderId, { text: fallback }, { quoted: message });  
      return;  
    }  
  
    const lang = detectLang(text);  
  
    await sock.sendPresenceUpdate('paused', senderId);  
  
    // Let AI craft the reply using the transcription as user input  
    await handleTextMessage(sock, { ...message, message: { ...message.message, conversation: text } }, senderId, senderName);  
  } catch {  
    await sock.sendPresenceUpdate('paused', senderId).catch(() => {});  
    await sock.sendMessage(senderId, { text: '⚠️ Voice error. Please try again or send text 🙏' }, { quoted: message });  
  }  
}  
  
async function handleImageMessage(sock, message, senderId, senderName) {  
  const order = botData.orders[senderId];  
  
  const lang = order?.language || 'roman_urdu';  
  
  if (order && order.status === 'pending') {  
    order.hasScreenshot = true;  
    requestSaveData();  
  
    const fallback = getScreenshotReceivedStatic(order.orderId, lang);  
  
    const scenarioDetails =  
      `Customer ne payment screenshot bhej diya hai.\n` +  
      `Order: #${order.orderId}\n` +  
      `Status: Admin is verifying.\n` +  
      `Delivery: 1 hour.\n` +  
      `Customer ko short message do (3-4 lines), emojis, do NOT include payment numbers, do NOT output ORDER_READY.`;  
  
    const aiMsg = await getAIScenarioMessage({  
      userId: senderId,  
      customerName: senderName,  
      lang,  
      scenarioName: 'PAYMENT_SCREENSHOT_RECEIVED',  
      scenarioDetails,  
      fallbackText: fallback  
    });  
  
    await sock.sendMessage(senderId, { text: aiMsg });  
  
    // Notify admin (internal)  
    const adminJid = getAdminJid();  
    if (adminJid) {  
      await sock  
        .sendMessage(adminJid, {  
          text:  
            `🔔 *New Payment Screenshot!*\n\n` +  
            `Order: *#${order.orderId}*\nCustomer: ${senderName}\nNumber: ${order.customerNumber}\n\n` +  
            `Dashboard pe approve/reject karo ⚡`  
        })  
        .catch(() => {});  
    }  
  
    return;  
  }  
  
  // No pending order -> treat as conversation (AI)  
  const aiReply = await getAISalesResponse('[Customer ne screenshot bheja bina pending order ke]', senderId, senderName, lang);  
  if (aiReply.message) await sock.sendMessage(senderId, { text: aiReply.message }, { quoted: message }).catch(() => {});  
}  
  
/* ─────────────────────────────  
   Start Bot  
───────────────────────────── */  
async function startBot() {  
  botStatus = 'starting';  
  
  try {  
    const { version } = await fetchLatestBaileysVersion();  
  
    currentQR = null;  
  
    ensureDirSync(WA_AUTH_DIR);  
    await restoreWhatsAppAuthFromUpstash();  
  
    const { state, saveCreds } = await useMultiFileAuthState(WA_AUTH_DIR);  
  
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
  
    sockGlobal = sock;  
  
    sock.ev.on('creds.update', () => {  
      try {  
        Promise.resolve(saveCreds()).catch(() => {});  
      } finally {  
        persistWhatsAppAuthToUpstash().catch(() => {});  
      }  
    });  
  
    sock.ev.on('connection.update', async (update) => {  
      const { connection, lastDisconnect, qr } = update;  
  
      if (qr) {  
        currentQR = qr;  
        botStatus = 'qr_ready';  
        console.log('✅ QR Ready! Open /qr to scan');  
      }  
  
      if (connection === 'close') {  
        currentQR = null;  
  
        const code = lastDisconnect?.error?.output?.statusCode;  
        console.log('❌ Connection closed:', code);  
  
        if (code === DisconnectReason.loggedOut) {  
          botStatus = 'logged_out';  
          try {  
            fs.rmSync(WA_AUTH_DIR, { recursive: true, force: true });  
          } catch {}  
  
          if (WHATSAPP_UPSTASH_CLEAR_ON_LOGGED_OUT) {  
            clearWhatsAppAuthInUpstash().catch(() => {});  
          }  
  
          setTimeout(() => startBot().catch(() => {}), 5000);  
          return;  
        }  
  
        botStatus = 'reconnecting';  
        setTimeout(() => startBot().catch(() => {}), code === 405 ? 15000 : 10000);  
      }  
  
      if (connection === 'open') {  
        botStatus = 'connected';  
        currentQR = null;  
        console.log('✅ WhatsApp Connected! Mega Agency LIVE');  
  
        initSheet().catch(() => {});  
      }  
    });  
  
    sock.ev.on('messages.upsert', async ({ messages, type }) => {  
      if (type !== 'notify') return;  
  
      for (const msg of messages) {  
        if (!msg?.key?.remoteJid) continue;  
        if (msg.key.fromMe) continue;  
  
        const senderId = msg.key.remoteJid;  
  
        if (senderId === 'status@broadcast') continue;  
        if (senderId.endsWith('@g.us')) continue;  
        if (senderId.endsWith('@broadcast')) continue;  
        if (senderId.includes('newsletter')) continue;  
  
        const senderName = msg.pushName || 'Customer';  
        const msgType = Object.keys(msg.message || {})[0];  
  
        enqueueForJid(senderId, async () => {  
          try {  
            if (msgType === 'audioMessage' || msgType === 'pttMessage') {  
              await handleVoiceMessage(sock, msg, senderId, senderName);  
              return;  
            }  
  
            if (msgType === 'imageMessage') {  
              await handleImageMessage(sock, msg, senderId, senderName);  
              return;  
            }  
  
            // default: treat as text if conversation/extendedText exists  
            await handleTextMessage(sock, msg, senderId, senderName);  
          } catch (e) {  
            console.error('Handle message error:', e?.message || e);  
          }  
        });  
      }  
    });  
  
    return;  
  } catch (err) {  
    console.error('Bot start error:', err?.message || err);  
    botStatus = 'error';  
    setTimeout(() => startBot().catch(() => {}), 15000);  
  }  
}  
  
/* ─────────────────────────────  
   DASHBOARD HTML (MINIMAL + SAFE)  
───────────────────────────── */  
function loginHtml(businessName) {  
  return (  
    '<!doctype html>' +  
    '<html><head><meta name="viewport" content="width=device-width,initial-scale=1"/>' +  
    '<style>' +  
    'body{background:#0f0f0f;color:#fff;font-family:Segoe UI,Arial,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;}' +  
    '.box{background:#1a1a1a;padding:28px;border-radius:14px;width:92%;max-width:420px;border:1px solid #333;text-align:center;}' +  
    'h1{color:#25D366;font-size:20px;margin:0 0 8px;}' +  
    'p{color:#aaa;font-size:13px;margin:0 0 18px;}' +  
    'input{width:100%;padding:12px 14px;background:#0f0f0f;border:1px solid #333;border-radius:10px;color:#fff;font-size:15px;outline:none;}' +  
    'input:focus{border-color:#25D366;}' +  
    'button{width:100%;margin-top:12px;padding:12px 14px;background:#25D366;border:none;border-radius:10px;color:#000;font-size:16px;font-weight:800;cursor:pointer;}' +  
    'button:hover{background:#1ebe57;}' +  
    '.err{color:#e74c3c;font-size:13px;margin-top:10px;display:none;}' +  
    '</style></head><body>' +  
    '<div class="box">' +  
    '<h1>🏪 ' +  
    (businessName || 'Mega Agency') +  
    '</h1>' +  
    '<p>Admin Dashboard Login</p>' +  
    '<input type="password" id="pass" placeholder="Password"/>' +  
    '<button onclick="login()">🔐 Login</button>' +  
    '<div class="err" id="err">❌ Wrong password!</div>' +  
    '</div>' +  
    '<script>' +  
    'async function login(){' +  
    ' const r=await fetch("/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({password:document.getElementById("pass").value})});' +  
    ' const d=await r.json();' +  
    ' if(d.success){location="/dashboard";}else{document.getElementById("err").style.display="block";}' +  
    '}' +  
    '</script>' +  
    '</body></html>'  
  );  
}  
  
function dashboardHtml() {  
  const biz = botData?.settings?.businessName || 'Mega Agency';  
  
  return (  
    '<!doctype html>' +  
    '<html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>' +  
    '<title>' + biz + ' - Admin</title>' +  
    '<style>' +  
    '*{box-sizing:border-box}body{margin:0;background:#0a0a0a;color:#e0e0e0;font-family:Segoe UI,Arial,sans-serif;}' +  
    '.wrap{max-width:1200px;margin:0 auto;padding:18px;}' +  
    '.top{display:flex;gap:12px;flex-wrap:wrap;align-items:center;justify-content:space-between;margin-bottom:12px;}' +  
    '.badge{padding:6px 12px;border-radius:999px;border:1px solid #333;font-weight:800;font-size:12px;}' +  
    '.live{background:#0d2b0d;color:#25D366;border-color:#25D366;}' +  
    '.off{background:#2b0d0d;color:#e74c3c;border-color:#e74c3c;}' +  
    '.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:14px;}' +  
    '.card{background:#111;border:1px solid #222;border-radius:12px;padding:14px;}' +  
    'h2{margin:0 0 10px;font-size:15px;}' +  
    'textarea{width:100%;min-height:160px;background:#0f0f0f;border:1px solid #333;border-radius:10px;color:#fff;padding:12px;outline:none;}' +  
    'input{width:100%;padding:10px 12px;background:#0f0f0f;border:1px solid #333;border-radius:10px;color:#fff;outline:none;}' +  
    'label{display:block;color:#aaa;font-size:12px;margin-top:10px;margin-bottom:6px;}' +  
    'button{background:#25D366;color:#000;border:none;border-radius:10px;padding:10px 14px;font-weight:900;cursor:pointer;}' +  
    'button.gray{background:#333;color:#fff}' +  
    'button.red{background:#e74c3c;color:#fff}' +  
    'button.blue{background:#3498db;color:#fff}' +  
    '.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}' +  
    '.muted{color:#aaa;font-size:13px}' +  
    '.order{border:1px solid #222;background:#0f0f0f;border-radius:10px;padding:10px;margin-top:10px;}' +  
    '.orderTop{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;}' +  
    '.small{font-size:12px;color:#aaa;margin-top:6px;line-height:1.5;white-space:pre-wrap}' +  
    '.toast{position:fixed;right:16px;bottom:16px;background:#25D366;color:#000;padding:12px 16px;border-radius:10px;font-weight:900;display:none;z-index:999}' +  
    'hr{border:none;border-top:1px solid #222;margin:14px 0}' +  
    '.ordersWrap{display:flex;gap:14px;flex-wrap:wrap}' +  
    '.ordersCol{flex:1;min-width:320px}' +  
    '</style></head>' +  
    '<body><div class="wrap">' +  
    '<div class="top">' +  
    '<div>' +  
    '<div style="font-weight:1000;font-size:18px;color:#25D366;margin-bottom:2px;">' + biz + ' Admin</div>' +  
    '<div class="muted" id="sub">Loading...</div>' +  
    '</div>' +  
    '<div class="row">' +  
    '<div class="badge off" id="botBadge">Bot: ...</div>' +  
    '<button class="gray" onclick="loadData()">🔄 reload</button>' +  
    '</div>' +  
    '</div>' +  
  
    '<div class="ordersWrap">' +  
    '<div class="ordersCol card"><h2>📦 Orders</h2><div class="muted">Pending</div><div id="pending"></div><hr/><div class="muted">Approved</div><div id="approved"></div><hr/><div class="muted">Rejected</div><div id="rejected"></div></div>' +  
    '</div>' +  
  
    '<div class="grid" style="margin-top:14px;">' +  
    '<div class="card"><h2>🤖 AI Prompt</h2>' +  
    '<textarea id="aiPrompt"></textarea>' +  
    '<div class="row" style="margin-top:12px;"><button onclick="savePrompt()">💾 Save Prompt</button></div>' +  
    '</div>' +  
  
    '<div class="card"><h2>⚙️ Settings</h2>' +  
    '<label>Business Name</label><input id="s_bizName"/>' +  
    '<label>Admin WhatsApp Number (e.g. 923001234567)</label><input id="s_adminNum"/>' +  
    '<label>Dashboard Password (leave empty to keep)</label><input id="s_password" type="password"/>' +  
    '<div class="row" style="margin-top:12px;"><button onclick="saveSettings()">💾 Save Settings</button></div>' +  
    '</div>' +  
  
    '<div class="card"><h2>💳 Payment</h2>' +  
    '<label>EasyPaisa Number</label><input id="ep_number"/>' +  
    '<label>EasyPaisa Name</label><input id="ep_name"/>' +  
    '<label>JazzCash Number</label><input id="jc_number"/>' +  
    '<label>JazzCash Name</label><input id="jc_name"/>' +  
    '<label>Bank Name</label><input id="bank_name"/>' +  
    '<label>Account Number</label><input id="bank_acc"/>' +  
    '<label>Account Holder Name</label><input id="bank_holder"/>' +  
    '<label>IBAN</label><input id="bank_iban"/>' +  
    '<div class="row" style="margin-top:12px;"><button onclick="savePayment()">💾 Save Payment</button></div>' +  
    '</div>' +  
  
    '<div class="card"><h2>🎨 Products (JSON Array)</h2>' +  
    '<div class="muted" style="margin-bottom:10px;">Paste products array and keep fields: id,name,price,description,features,downloadLink,active</div>' +  
    '<textarea id="productsJson"></textarea>' +  
    '<div class="row" style="margin-top:12px;"><button onclick="saveProducts()">💾 Save Products</button></div>' +  
    '</div>' +  
    '</div>' +  
    '</div>' +  
  
    '<div class="toast" id="toast"></div>' +  
  
    '<script>' +  
    'let allData=null;' +  
    'function $(id){return document.getElementById(id)}' +  
    'function toast(msg){const t=$(\"toast\");t.textContent=msg;t.style.display=\"block\";setTimeout(()=>t.style.display=\"none\",2500)}' +  
    'function setBadge(status){const b=$(\"botBadge\"); if(status===\"connected\"){b.className=\"badge live\";b.textContent=\"Bot: connected\";}else{b.className=\"badge off\";b.textContent=\"Bot: \"+status;}}' +  
    'function orderCard(o){' +  
    '  const time=o.timestamp?new Date(o.timestamp).toLocaleString(\"en-PK\"):\"\";' +  
    '  const shot=o.hasScreenshot?\"✅ Received\":\"❌ Pending\";' +  
    '  const lang=o.language||\"\";' +  
    '  if(o.status===\"pending\"){' +  
    '    return ' +  
    '      \"<div class=\\\"order\\\">\"+' +  
    '      \"<div class=\\\"orderTop\\\">\"+' +  
    '        \"<div><b>#\"+o.orderId+\"</b>\"+ (lang?\" <span class=\\\"muted\\\">(\"+lang+\")</span>\":\"\") +\"<div class=\\\"small\\\">\"+(o.customerName||\"\")+\" • \"+(o.customerNumber||\"\") +\"</div></div>\"+' +  
    '        \"<div class=\\\"row\\\" style=\\\"justify-content:flex-end\\\">\"+' +  
    '          \"<button class=\\\"blue\\\" style=\\\"margin-right:8px\\\" onclick=\\\"approve(\"+o.orderId+\")\\\">✅ Approve</button>\"+' +  
    '          \"<button class=\\\"red\\\" onclick=\\\"reject(\"+o.orderId+\")\\\">❌ Reject</button>\"+' +  
    '        \"</div>\"+' +  
    '      \"</div>\"+' +  
    '      \"<div class=\\\"small\\\">Product: <b>\"+(o.productName||\"\")+\"</b>\\nScreenshot: \"+shot+\"\\nTime: \"+time+\"</div>\"+' +  
    '      \"</div>\";' +  
    '  }' +  
    '  if(o.status===\"approved\"){' +  
    '    return ' +  
    '      \"<div class=\\\"order\\\">\"+' +  
    '      \"<div class=\\\"orderTop\\\">\"+' +  
    '        \"<div><b>#\"+o.orderId+\"</b><div class=\\\"small\\\">\"+(o.customerName||\"\")+\" • \"+(o.customerNumber||\"\")+\"</div></div>\"+' +  
    '        \"<div class=\\\"muted\\\">✅ Approved</div>\"+' +  
    '      \"</div>\"+' +  
    '      \"<div class=\\\"small\\\">Product: <b>\"+(o.productName||\"\")+\"</b>\\nScreenshot: \"+shot+\"\\nTime: \"+time+\"</div>\"+' +  
    '      \"</div>\";' +  
    '  }' +  
    '  return ' +  
    '    \"<div class=\\\"order\\\">\"+' +  
    '    \"<div class=\\\"orderTop\\\">\"+' +  
    '      \"<div><b>#\"+o.orderId+\"</b><div class=\\\"small\\\">\"+(o.customerName||\"\")+\" • \"+(o.customerNumber||\"\")+\"</div></div>\"+' +  
    '      \"<div class=\\\"muted\\\">❌ Rejected</div>\"+' +  
    '    \"</div>\"+' +  
    '    \"<div class=\\\"small\\\">Product: <b>\"+(o.productName||\"\")+\"</b>\\nTime: \"+time+\"</div>\"+' +  
    '    \"</div>\";' +  
    '}' +  
    'async function api(path,opts){const r=await fetch(path,opts); return r.json();}' +  
    'async function loadData(){' +  
    '  const d=await api(\"/api/data\");' +  
    '  allData=d;' +  
    '  $(\"sub\").textContent = \"Orders: \"+d.stats.total+\" • Pending: \"+d.stats.pending+\" • Approved: \"+d.stats.approved+\" • Rejected: \"+d.stats.rejected;' +  
    '  setBadge(d.botStatus);' +  
    '  $(\"aiPrompt\").value = d.aiPrompt || \"\";' +  
    '  $(\"s_bizName\").value = d.settings?.businessName || \"\";' +  
    '  $(\"s_adminNum\").value = d.settings?.adminNumber || \"\";' +  
    '  $(\"s_password\").value = \"\";' +  
    '  $(\"ep_number\").value = d.payment?.easypaisa?.number || \"\";' +  
    '  $(\"ep_name\").value = d.payment?.easypaisa?.name || \"\";' +  
    '  $(\"jc_number\").value = d.payment?.jazzcash?.number || \"\";' +  
    '  $(\"jc_name\").value = d.payment?.jazzcash?.name || \"\";' +  
    '  $(\"bank_name\").value = d.payment?.bank?.bankName || \"\";' +  
    '  $(\"bank_acc\").value = d.payment?.bank?.accountNumber || \"\";' +  
    '  $(\"bank_holder\").value = d.payment?.bank?.accountName || \"\";' +  
    '  $(\"bank_iban\").value = d.payment?.bank?.iban || \"\";' +  
    '  $(\"productsJson\").value = JSON.stringify(d.products || [], null, 2);' +  
    '  const orders=(d.orders||[]);' +  
    '  const pending=orders.filter(o=>o.status===\"pending\");' +  
    '  const approved=orders.filter(o=>o.status===\"approved\");' +  
    '  const rejected=orders.filter(o=>o.status===\"rejected\");' +  
    '  $(\"pending\").innerHTML = pending.length ? pending.map(orderCard).join(\"\") : \"<div class=\\\"muted\\\">No pending order</div>\";' +  
    '  $(\"approved\").innerHTML = approved.length ? approved.map(orderCard).join(\"\") : \"<div class=\\\"muted\\\">No approved order</div>\";' +  
    '  $(\"rejected\").innerHTML = rejected.length ? rejected.map(orderCard).join(\"\") : \"<div class=\\\"muted\\\">No rejected order</div>\";' +  
    '}' +  
    'async function savePrompt(){' +  
    '  await api(\"/api/prompt\",{method:\"POST\",headers:{\"Content-Type\":\"application/json\"},body:JSON.stringify({prompt:$(\"aiPrompt\").value})});' +  
    '  toast(\"✅ Prompt saved\");' +  
    '}' +  
    'async function saveSettings(){' +  
    '  await api(\"/api/settings\",{method:\"POST\",headers:{\"Content-Type\":\"application/json\"},body:JSON.stringify({' +  
    '    businessName:$(\"s_bizName\").value,' +  
    '    adminNumber:$(\"s_adminNum\").value,' +  
    '    dashboardPassword:$(\"s_password\").value' +  
    '  })});' +  
    '  $(\"s_password\").value=\"\";' +  
    '  toast(\"✅ Settings saved\");' +  
    '  await loadData();' +  
    '}' +  
    'async function savePayment(){' +  
    '  await api(\"/api/payment\",{method:\"POST\",headers:{\"Content-Type\":\"application/json\"},body:JSON.stringify({'+  
    '    easypaisa:{number:$(\"ep_number\").value,name:$(\"ep_name\").value},' +  
    '    jazzcash:{number:$(\"jc_number\").value,name:$(\"jc_name\").value},' +  
    '    bank:{bankName:$(\"bank_name\").value,accountNumber:$(\"bank_acc\").value,accountName:$(\"bank_holder\").value,iban:$(\"bank_iban\").value}' +  
    '  })});' +  
    '  toast(\"✅ Payment saved\");' +  
    '  await loadData();' +  
    '}' +  
    'async function saveProducts(){' +  
    '  let arr=null;' +  
    '  try{ arr=JSON.parse($(\"productsJson\").value||\"[]\"); }catch(e){ toast(\"Products JSON invalid\"); return; }' +  
    '  if(!Array.isArray(arr)){ toast(\"Products must be array\"); return; }' +  
    '  await api(\"/api/products\",{method:\"POST\",headers:{\"Content-Type\":\"application/json\"},body:JSON.stringify(arr)});' +  
    '  toast(\"✅ Products saved\");' +  
    '  await loadData();' +  
    '}' +  
    'async function approve(orderId){' +  
    '  if(!confirm(\"Approve order #\"+orderId+\"?\")) return;' +  
    '  await api(\"/api/approve/\"+orderId,{method:\"POST\"});' +  
    '  toast(\"✅ Approved\");' +  
    '  await loadData();' +  
    '}' +  
    'async function reject(orderId){' +  
    '  if(!confirm(\"Reject order #\"+orderId+\"?\")) return;' +  
    '  await api(\"/api/reject/\"+orderId,{method:\"POST\"});' +  
    '  toast(\"❌ Rejected\");' +  
    '  await loadData();' +  
    '}' +  
    'loadData().catch(()=>{ $(\"sub\").textContent=\"Failed to load. Check server.\"; });' +  
    '</script>' +  
    '</body></html>'  
  );  
}  
  
/* ─────────────────────────────  
   SERVER AUTH + ROUTES  
───────────────────────────── */  
const sessions = {};  
  
function isAuthenticated(req) {  
  const cookies = req.headers.cookie || '';  
  const sessionMatch = cookies.match(/session=([^;]+)/);  
  if (!sessionMatch) return false;  
  return sessions[sessionMatch[1]] === true;  
}  
  
function parseBody(req) {  
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
  
async function screenshotReceivedAI(order, customerName) {  
  const lang = order.language || 'roman_urdu';  
  const fallback = getScreenshotReceivedStatic(order.orderId, lang);  
  
  const scenarioDetails =  
    `Customer ne payment screenshot bhej diya hai.\n` +  
    `Order: #${order.orderId}\n` +  
    `Admin is verifying.\n` +  
    `Delivery: within 1 hour.\n` +  
    `Write customer message in ${lang}. Do NOT include payment numbers.`;  
  
  return getAIScenarioMessage({  
    userId: order.customerJid,  
    customerName,  
    lang,  
    scenarioName: 'PAYMENT_SCREENSHOT_RECEIVED',  
    scenarioDetails,  
    fallbackText: fallback  
  });  
}  
  
async function paymentApprovedAI(order, product, customerName) {  
  const lang = order.language || 'roman_urdu';  
  const fallback = getPaymentApprovedStatic(order, product, lang);  
  
  const downloadPart = product?.downloadLink ? `Download link exists.` : `No download link.`;  
  const scenarioDetails =  
    `Payment approved and order confirmed.\nOrder #${order.orderId}\nProduct: ${product?.name}\n${downloadPart}\n` +  
    `If downloadLink exists, include it.\nKeep short (3-5 lines), emojis ok. Do NOT output ORDER_READY.`;  
  
  return getAIScenarioMessage({  
    userId: order.customerJid,  
    customerName,  
    lang,  
    scenarioName: 'PAYMENT_APPROVED',  
    scenarioDetails,  
    fallbackText: fallback  
  });  
}  
  
async function paymentRejectedAI(order, customerName) {  
  const lang = order.language || 'roman_urdu';  
  const fallback = getPaymentRejectedStatic(order, lang);  
  
  const scenarioDetails =  
    `Payment verify failed.\nOrder #${order.orderId}\nApologize politely.\nAsk customer to resend correct screenshot or contact admin.\n` +  
    `Also mention: type "buy" to retry.\nKeep short (3-5 lines), emojis ok. Do NOT output ORDER_READY.`;  
  
  return getAIScenarioMessage({  
    userId: order.customerJid,  
    customerName,  
    lang,  
    scenarioName: 'PAYMENT_REJECTED',  
    scenarioDetails,  
    fallbackText: fallback  
  });  
}  
  
const server = http.createServer(async (req, res) => {  
  try {  
    const parsed = url.parse(req.url, true);  
    const pathname = parsed.pathname;  
    const method = req.method || 'GET';  
  
    // Health check  
    if (pathname === '/health') {  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ ok: true, botStatus }));  
      return;  
    }  
  
    // Login page  
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
          res.end(JSON.stringify({ success: false }));  
        }  
        return;  
      }  
  
      res.writeHead(200, { 'Content-Type': 'text/html' });  
      res.end(loginHtml(botData?.settings?.businessName));  
      return;  
    }  
  
    // QR page (no auth)  
    if (pathname === '/qr') {  
      res.writeHead(200, { 'Content-Type': 'text/html' });  
  
      if (botStatus === 'connected') {  
        res.end('<!doctype html><html><body style="font-family:Segoe UI,Arial,sans-serif;background:#111;color:#fff;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;text-align:center;">' +  
          '<h2 style="color:#25D366;margin:0 0 10px;">✅ Bot Connected!</h2>' +  
          '<p style="color:#aaa;margin:0;">Open /dashboard</p>' +  
          '</body></html>');  
        return;  
      }  
  
      if (!currentQR) {  
        res.end('<!doctype html><html><body style="font-family:Segoe UI,Arial,sans-serif;background:#111;color:#fff;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;text-align:center;">' +  
          '<h2 style="color:#f39c12;margin:0 0 10px;">⏳ QR not ready</h2>' +  
          '<p style="color:#aaa;margin:0;">Status: ' + botStatus + '</p>' +  
          '</body></html>');  
        return;  
      }  
  
      try {  
        const qrDataURL = await QRCode.toDataURL(currentQR, { width: 300, margin: 2 });  
        res.end(  
          '<!doctype html><html><head><meta http-equiv="refresh" content="25"/></head><body style="font-family:Segoe UI,Arial,sans-serif;background:#111;color:#fff;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;text-align:center;padding:20px;">' +  
          '<h2 style="color:#25D366;margin:0 0 10px;">📱 WhatsApp QR</h2>' +  
          '<img src="' + qrDataURL + '" style="border:8px solid white;border-radius:12px;width:280px;height:280px;"/>' +  
          '<p style="color:#f39c12;margin-top:15px;">⚠️ QR expires in ~25 sec</p>' +  
          '<p style="color:#aaa;margin-top:6px;">Scan to connect bot.</p>' +  
          '</body></html>'  
        );  
      } catch (e) {  
        res.end('<h1 style="color:red;font-family:Segoe UI,Arial,sans-serif;">QR Error: ' + (e?.message || e) + '</h1>');  
      }  
      return;  
    }  
  
    // Auth check for everything else  
    if (pathname !== '/qr' && pathname !== '/login' && pathname !== '/health' && !isAuthenticated(req)) {  
      res.writeHead(302, { Location: '/login' });  
      res.end();  
      return;  
    }  
  
    // Dashboard  
    if (pathname === '/dashboard' || pathname === '/') {  
      res.writeHead(200, { 'Content-Type': 'text/html' });  
      res.end(dashboardHtml());  
      return;  
    }  
  
    // API: get data  
    if (pathname === '/api/data' && method === 'GET') {  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
  
      const ordersArr = Object.values(botData.orders || {});  
      const MAX_ORDERS = parseInt(process.env.MAX_ORDERS_TO_RETURN || '80', 10);  
  
      ordersArr.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));  
      const orders = ordersArr.slice(0, MAX_ORDERS).map((o) => {  
        const product = (botData.products || []).find((p) => p.id === o.productId) || (botData.products || [])[0];  
        return {  
          orderId: o.orderId,  
          customerJid: o.customerJid,  
          customerNumber: o.customerNumber,  
          customerName: o.customerName,  
          productId: o.productId,  
          productName: product?.name || '',  
          language: o.language,  
          status: o.status,  
          hasScreenshot: !!o.hasScreenshot,  
          timestamp: o.timestamp  
        };  
      });  
  
      const pending = ordersArr.filter((o) => o.status === 'pending').length;  
      const approved = ordersArr.filter((o) => o.status === 'approved').length;  
      const rejected = ordersArr.filter((o) => o.status === 'rejected').length;  
  
      let revenue = 0;  
      for (const o of ordersArr) {  
        if (o.status !== 'approved') continue;  
        const p = (botData.products || []).find((x) => x.id === o.productId);  
        revenue += p?.price || 0;  
      }  
  
      res.end(  
        JSON.stringify({  
          botStatus,  
          settings: botData.settings,  
          payment: botData.payment,  
          products: botData.products,  
          aiPrompt: botData.aiPrompt,  
          orders,  
          stats: {  
            pending,  
            approved,  
            rejected,  
            total: ordersArr.length,  
            revenue  
          }  
        })  
      );  
      return;  
    }  
  
    // API: update prompt  
    if (pathname === '/api/prompt' && method === 'POST') {  
      const body = await parseBody(req);  
      botData.aiPrompt = typeof body.prompt === 'string' ? body.prompt : botData.aiPrompt;  
      requestSaveData(0);  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: true }));  
      return;  
    }  
  
    // API: update settings  
    if (pathname === '/api/settings' && method === 'POST') {  
      const b = await parseBody(req);  
  
      if (typeof b.businessName === 'string') botData.settings.businessName = b.businessName;  
      if (typeof b.adminNumber === 'string') botData.settings.adminNumber = b.adminNumber;  
      if (typeof b.dashboardPassword === 'string' && b.dashboardPassword.trim()) {  
        botData.settings.dashboardPassword = b.dashboardPassword.trim();  
      }  
  
      requestSaveData(0);  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: true }));  
      return;  
    }  
  
    // API: update payment  
    if (pathname === '/api/payment' && method === 'POST') {  
      const b = await parseBody(req);  
  
      if (b?.easypaisa?.number) botData.payment.easypaisa.number = b.easypaisa.number;  
      if (b?.easypaisa?.name) botData.payment.easypaisa.name = b.easypaisa.name;  
  
      if (b?.jazzcash?.number) botData.payment.jazzcash.number = b.jazzcash.number;  
      if (b?.jazzcash?.name) botData.payment.jazzcash.name = b.jazzcash.name;  
  
      if (b?.bank?.bankName) botData.payment.bank.bankName = b.bank.bankName;  
      if (b?.bank?.accountNumber) botData.payment.bank.accountNumber = b.bank.accountNumber;  
      if (b?.bank?.accountName) botData.payment.bank.accountName = b.bank.accountName;  
      if (b?.bank?.iban) botData.payment.bank.iban = b.bank.iban;  
  
      requestSaveData(0);  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: true }));  
      return;  
    }  
  
    // API: update products  
    if (pathname === '/api/products' && method === 'POST') {  
      const products = await parseBody(req);  
      if (!Array.isArray(products)) {  
        res.writeHead(400, { 'Content-Type': 'application/json' });  
        res.end(JSON.stringify({ success: false, error: 'products must be array' }));  
        return;  
      }  
      // minimal normalization  
      botData.products = products.map((p, idx) => ({  
        id: p.id ?? idx + 1,  
        name: p.name || `Product ${idx + 1}`,  
        price: Number(p.price || 0),  
        description: p.description || '',  
        features: Array.isArray(p.features) ? p.features : [],  
        downloadLink: p.downloadLink || '',  
        active: !!p.active  
      }));  
      if (!botData.products.some((p) => p.active)) {  
        botData.products[0].active = true;  
      }  
  
      requestSaveData(0);  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: true }));  
      return;  
    }  
  
    // API: approve/reject  
    if (pathname.startsWith('/api/approve/') && method === 'POST') {  
      const orderId = parseInt(pathname.split('/api/approve/')[1], 10);  
      const order = Object.values(botData.orders || {}).find((o) => o.orderId === orderId);  
  
      if (order && sockGlobal) {  
        order.status = 'approved';  
        requestSaveData(0);  
  
        const product =  
          (botData.products || []).find((p) => p.id === order.productId) || (botData.products || [])[0];  
  
        try {  
          const msg = await paymentApprovedAI(order, product, order.customerName || 'Customer');  
          await sockGlobal.sendMessage(order.customerJid, { text: msg });  
        } catch (e) {  
          await sockGlobal.sendMessage(order.customerJid, { text: getPaymentApprovedStatic(order, product, order.language || 'roman_urdu') }).catch(() => {});  
        }  
  
        saveToSheet({  
          orderId: order.orderId,  
          customerName: order.customerName,  
          customerNumber: order.customerNumber,  
          product: product?.name || '',  
          amount: product?.price || '',  
          status: 'approved',  
          language: order.language  
        }).catch(() => {});  
      }  
  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: true }));  
      return;  
    }  
  
    if (pathname.startsWith('/api/reject/') && method === 'POST') {  
      const orderId = parseInt(pathname.split('/api/reject/')[1], 10);  
      const order = Object.values(botData.orders || {}).find((o) => o.orderId === orderId);  
  
      if (order && sockGlobal) {  
        order.status = 'rejected';  
        requestSaveData(0);  
  
        try {  
          const msg = await paymentRejectedAI(order, order.customerName || 'Customer');  
          await sockGlobal.sendMessage(order.customerJid, { text: msg });  
        } catch {  
          await sockGlobal.sendMessage(order.customerJid, { text: getPaymentRejectedStatic(order, order.language || 'roman_urdu') }).catch(() => {});  
        }  
  
        saveToSheet({  
          orderId: order.orderId,  
          customerName: order.customerName,  
          customerNumber: order.customerNumber,  
          product: (botData.products || []).find((p) => p.id === order.productId)?.name || '',  
          amount: (botData.products || []).find((p) => p.id === order.productId)?.price || '',  
          status: 'rejected',  
          language: order.language  
        }).catch(() => {});  
      }  
  
      res.writeHead(200, { 'Content-Type': 'application/json' });  
      res.end(JSON.stringify({ success: true }));  
      return;  
    }  
  
    // Logout  
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
  } catch (e) {  
    console.error('Server handler error:', e?.message || e);  
    res.writeHead(500, { 'Content-Type': 'application/json' });  
    res.end(JSON.stringify({ error: 'Internal server error' }));  
  }  
});  
  
/* ─────────────────────────────  
   BOOT  
───────────────────────────── */  
server.listen(PORT, () => {  
  console.log(`🚀 Server ready on port ${PORT}`);  
  console.log('   Open /login then /dashboard. Open /qr for WhatsApp QR.');  
});  
  
// Start server ASAP, then load data and start bot in background (prevents Render 502 at boot)  
loadData()  
  .then(() => console.log('✅ botData loaded'))  
  .catch(() => {});  
startBot().catch(() => {});  
