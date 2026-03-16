"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dlmm_1 = __importStar(require("@meteora-ag/dlmm"));
const web3_js_1 = require("@solana/web3.js");
const bs58_1 = __importDefault(require("bs58"));
const dotenv = __importStar(require("dotenv"));
const anchor_1 = require("@coral-xyz/anchor");
dotenv.config({ path: '../.env' });
async function main() {
    const args = process.argv.slice(2);
    const poolAddress = args[0];
    const baseAmountRaw = args[1]; // Lamports
    const quoteAmountRaw = args[2]; // Minor units
    if (!poolAddress || !baseAmountRaw || !quoteAmountRaw) {
        console.error("Usage: ts-node deposit_dlmm.ts <pool_address> <base_lamports> <quote_raw>");
        process.exit(1);
    }
    const rpcUrl = process.env.SOLANA_RPC_URL || "https://api.mainnet-beta.solana.com";
    const connection = new web3_js_1.Connection(rpcUrl, "confirmed");
    const pkey = process.env.WALLET_PRIVATE_KEY_BASE58;
    if (!pkey) {
        console.error("Missing WALLET_PRIVATE_KEY_BASE58 in .env");
        process.exit(1);
    }
    const wallet = web3_js_1.Keypair.fromSecretKey(bs58_1.default.decode(pkey));
    console.log(`🔑 Wallet loaded: ${wallet.publicKey.toBase58()}`);
    console.log(`🌊 Connecting to Meteora DLMM: ${poolAddress}`);
    try {
        const dlmm = await dlmm_1.default.create(connection, new web3_js_1.PublicKey(poolAddress));
        const activeBin = await dlmm.getActiveBin();
        console.log(`📊 Active Bin ID: ${activeBin.binId}`);
        // Define a Spot strategy around the active bin
        const binRange = 10; // spread bins
        const minBinId = activeBin.binId - binRange;
        const maxBinId = activeBin.binId + binRange;
        // Build bin IDs array
        const binIds = [];
        for (let i = minBinId; i <= maxBinId; i++) {
            binIds.push(i);
        }
        console.log(`🛠️ Building dynamic spot distribution [${minBinId} to ${maxBinId}]`);
        const xYAmountDistribution = (0, dlmm_1.calculateSpotDistribution)(activeBin.binId, binIds);
        // Position account keypair
        const positionKeypair = web3_js_1.Keypair.generate();
        const txConfig = {
            positionPubKey: positionKeypair.publicKey,
            totalXAmount: new anchor_1.BN(baseAmountRaw),
            totalYAmount: new anchor_1.BN(quoteAmountRaw),
            xYAmountDistribution: xYAmountDistribution,
            user: wallet.publicKey,
            slippage: 1, // 1%
        };
        const tx = await dlmm.initializePositionAndAddLiquidityByWeight(txConfig);
        console.log("📝 Transaction built, signing and sending...");
        const transactions = Array.isArray(tx) ? tx : [tx];
        for (const t of transactions) {
            const txSig = await connection.sendTransaction(t, [wallet, positionKeypair], { skipPreflight: false });
            console.log(`✅ Success! Sig: ${txSig}`);
        }
    }
    catch (err) {
        console.error("❌ DLMM Deposit Failed:", err);
        process.exit(1);
    }
}
main();
