import DLMM, { calculateSpotDistribution } from '@meteora-ag/dlmm';
import { Connection, Keypair, PublicKey } from '@solana/web3.js';
import bs58 from 'bs58';
import * as dotenv from 'dotenv';
import { BN } from '@coral-xyz/anchor';

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
    const connection = new Connection(rpcUrl, "confirmed");

    const pkey = process.env.WALLET_PRIVATE_KEY_BASE58;
    if (!pkey) {
        console.error("Missing WALLET_PRIVATE_KEY_BASE58 in .env");
        process.exit(1);
    }

    const wallet = Keypair.fromSecretKey(bs58.decode(pkey));
    console.log(`🔑 Wallet loaded: ${wallet.publicKey.toBase58()}`);
    console.log(`🌊 Connecting to Meteora DLMM: ${poolAddress}`);

    try {
        const dlmm = await DLMM.create(connection, new PublicKey(poolAddress));
        
        const activeBin = await dlmm.getActiveBin();
        console.log(`📊 Active Bin ID: ${activeBin.binId}`);

        // Define a Spot strategy around the active bin
        const binRange = 10; // spread bins
        const minBinId = activeBin.binId - binRange;
        const maxBinId = activeBin.binId + binRange;

        // Build bin IDs array
        const binIds: number[] = [];
        for (let i = minBinId; i <= maxBinId; i++) {
            binIds.push(i);
        }

        console.log(`🛠️ Building dynamic spot distribution [${minBinId} to ${maxBinId}]`);
        const xYAmountDistribution = calculateSpotDistribution(activeBin.binId, binIds);

        // Position account keypair
        const positionKeypair = Keypair.generate();

        const txConfig = {
            positionPubKey: positionKeypair.publicKey,
            totalXAmount: new BN(baseAmountRaw),
            totalYAmount: new BN(quoteAmountRaw),
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

    } catch (err: any) {
        console.error("❌ DLMM Deposit Failed:", err);
        process.exit(1);
    }
}

main();
