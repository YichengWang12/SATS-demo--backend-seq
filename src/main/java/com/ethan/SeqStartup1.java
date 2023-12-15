package com.ethan;

import com.ethan.bean.SeqConfig;
import lombok.extern.log4j.Log4j2;
import thirdpart.codec.IByteCodecImpl;

@Log4j2
public class SeqStartup1 {
    public static void main(String[] args) throws Exception {
        String configName = "seq1.properties";
        new SeqConfig(configName,new IByteCodecImpl()).startup();
    }
}
