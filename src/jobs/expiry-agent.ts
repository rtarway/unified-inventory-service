import { InventoryService } from '../services/inventory-service';

const inventoryService = new InventoryService();

async function runCallback() {
    try {
        console.log('Running Expiry Agent...');
        const count = await inventoryService.expireReservations();
        if (count > 0) {
            console.log(`Processed ${count} expired reservations.`);
        }
    } catch (error) {
        console.error('Error in Expiry Agent:', error);
    }
}

export async function startExpiryAgent(intervalMs: number = 60000) {
    console.log('Starting Expiry Agent...');
    await inventoryService.init();

    // Run immediately on start
    await runCallback();

    // Schedule
    setInterval(runCallback, intervalMs);
}

// Standalone execution if run directly
if (require.main === module) {
    const runOnce = process.argv.includes('--run-once');
    if (runOnce) {
        // K8s CronJob Mode: Run once and exit
        (async () => {
            console.log('Starting Expiry Agent (Run-Once Mode)...');
            await inventoryService.init();
            await runCallback();
            console.log('Expiry Agent finished. Exiting.');
            process.exit(0);
        })().catch(err => {
            console.error(err);
            process.exit(1);
        });
    } else {
        // Daemon Mode
        startExpiryAgent().catch(console.error);
    }
}
