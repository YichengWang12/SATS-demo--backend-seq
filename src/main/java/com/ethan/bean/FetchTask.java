package com.ethan.bean;

import com.alipay.lookout.common.top.TopUtil;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import io.vertx.core.buffer.Buffer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang.ArrayUtils;
import thirdpart.bean.Cmdpack;
import thirdpart.fetchserv.IFetchService;
import thirdpart.order.OrderCmd;
import thirdpart.order.OrderDirection;

import java.util.Map;
import java.util.List;
import java.util.TimerTask;

@Log4j2
@RequiredArgsConstructor
public class FetchTask extends TimerTask {
    @NonNull
    private SeqConfig config;

    @Override
    public void run() {
        //traverse gateways and get data
        if(!config.getNode().isLeader()){
            return;
        }

        Map<String , IFetchService> fetchServiceMap = config.getFetchServiceMap();
        if(MapUtils.isEmpty(fetchServiceMap)){
            return;
        }
        List<OrderCmd> cmds = collectAllOrders(fetchServiceMap);
        if(CollectionUtils.isEmpty(cmds)){
            return;
        }

        log.info(cmds);

        //sort
        //by timestamp / price / amount
        cmds.sort(((o1, o2) -> {
            int res = compareTime(o1,o2);
            if(res !=0){
                return res;
            }
            res = comparePrice(o1,o2);
            if(res != 0){
                return res;
            }
            res = compareAmount(o1,o2);
            return res;
        }));

        // store into KV STORE, send to core.
        //1. generate packNo
        //2. store into KV
        //3. update packNo
        //4. send
        try{
            long packNo = getPackNoFromStore();

            Cmdpack pack = new Cmdpack(packNo,cmds);
            byte[] serialize = config.getCodec().seriallize(pack);
            insertIntoKvStore(packNo,serialize);

            updatePackInStore(packNo+1);

            config.getMulticastSender().send(
                    Buffer.buffer(serialize),
                    config.getMulticastPort(),
                    config.getMulticastIp(),
                    null
            );

        }catch (Exception e){
            log.error("Error occur when storing when sending:",e);
        }
    }

    private void updatePackInStore(long packNo) {
        final byte[] key = new byte[8];
        Bits.putLong(key,0,packNo);
        config.getNode().getRheaKVStore().put(PACK_NO_KEY,key);
    }

    private void insertIntoKvStore(long packNo, byte[] seriallize) {
        byte[] key = new byte[8];
        Bits.putLong(key,0,packNo);
        config.getNode().getRheaKVStore().put(key,seriallize);
    }

    private static final byte[] PACK_NO_KEY = BytesUtil.writeUtf8("seq_pack_no");
    private long getPackNoFromStore() {
        final byte[] bPackNo = config.getNode().getRheaKVStore().bGet(PACK_NO_KEY);
        long packNo = 0;
        if(ArrayUtils.isNotEmpty(bPackNo)){
            packNo = Bits.getLong(bPackNo,0);
        }
        return packNo;
    }

    private int compareAmount(OrderCmd o1, OrderCmd o2) {
        if(o1.volume > o2.volume){
            return -1;
        } else if (o1.volume < o2.volume) {
            return 1;
        }else {
            return 0;
        }
    }

    private int compareTime(OrderCmd o1, OrderCmd o2) {
        if(o1.timestamp > o2.timestamp){
            return 1;
        }else if(o1.timestamp < o2.timestamp){
            return -1;
        }else{
            return 0;
        }
    }

    private int comparePrice(OrderCmd o1, OrderCmd o2){
        if(o1.direction == o2.direction){
            if(o1.price > o2.price){
                return o1.direction == OrderDirection.BUY? -1:1;
            } else if (o1.price < o2.price) {
                return o1.direction == OrderDirection.BUY? 1:-1;
            }
        }else{
            return 0;
        }
        return 0;
    }
    private List<OrderCmd> collectAllOrders(Map<String,IFetchService> fetchServiceMap){

        List<OrderCmd> msgs = Lists.newArrayList();
        fetchServiceMap.values().forEach(t ->{
            List<OrderCmd> orderCmds = t.fetchData();
            if(CollectionUtils.isNotEmpty(orderCmds)){
                msgs.addAll(orderCmds);
            }
        });
        return msgs;
    }
}
