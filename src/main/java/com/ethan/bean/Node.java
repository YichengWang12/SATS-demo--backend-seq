package com.ethan.bean;

import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicLong;

@Log4j2
@RequiredArgsConstructor
public class Node {

    @NonNull
    private final RheaKVStoreOptions options;
    @Getter
    private RheaKVStore rheaKVStore;
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader(){
        return leaderTerm.get() > 0;
    }

    /**
     * stop node
     */

    public void stop(){
        rheaKVStore.shutdown();
    }

    /**
     * start node
     */

    public void start(){

        //initialize
        rheaKVStore = new DefaultRheaKVStore();
        rheaKVStore.init(this.options);

        //listen the status
        rheaKVStore.addLeaderStateListener(-1, new LeaderStateListener() {
            @Override
            public void onLeaderStart(long newTerm) {
                log.info("Node has become a leader");
                leaderTerm.set(newTerm);
            }

            @Override
            public void onLeaderStop(long l) {
                leaderTerm.set(-1);
            }
        });

    }
}
