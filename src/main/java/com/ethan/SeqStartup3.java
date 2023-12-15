package com.ethan;

import com.ethan.bean.SeqConfig;
import thirdpart.codec.IByteCodecImpl;

public class SeqStartup3 {
    public static void main(String[] args) throws Exception {
        String configName = "seq3.properties";
        new SeqConfig(configName,new IByteCodecImpl()).startup();
    }
}
