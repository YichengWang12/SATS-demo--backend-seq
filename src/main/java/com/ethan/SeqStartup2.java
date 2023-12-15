package com.ethan;

import com.ethan.bean.SeqConfig;
import thirdpart.codec.IByteCodecImpl;

public class SeqStartup2 {
    public static void main(String[] args) throws Exception {
        String configName = "seq2.properties";
        new SeqConfig(configName,new IByteCodecImpl()).startup();
    }
}
