package com.vipxf.regulation.ilv;

import com.vipxf.regulation.ilv.core.Context;
import com.vipxf.regulation.ilv.core.YamlProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IlvApplication {

    public static void main(String[] args) {
        SpringApplication.run(IlvApplication.class, args);

        YamlProcessor p = new YamlProcessor();
        Context context = new Context("c:/data/desktop", "LOABD7002851000012202306s.zip");
        context.put("journal.source.JJFSE", "LOAFD7002851000012202306s.dat");
        context.put("journal.target.JJFSE", "JJFSE.csv");
        context.put("cbf.source.KHYE", "LOABD7002851000012202305s.dat");
        context.put("cbf.target.KHYE", "KHYE5.csv");
        context.put("lbf.source.KHYE", "LOABD7002851000012202306s.dat");
        context.put("lbf.target.KHYE", "KHYE6.csv");
        p.proc(context, "config.yml");
    }
}
