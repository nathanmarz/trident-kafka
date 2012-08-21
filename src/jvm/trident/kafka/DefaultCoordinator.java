package trident.kafka;

public class DefaultCoordinator implements IBatchCoordinator {

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {
    }
    
}
