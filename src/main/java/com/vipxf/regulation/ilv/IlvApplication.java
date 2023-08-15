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
        Context context = new Context("c:/data/desktop/LOAFD7002851000012202306s.zip");
        context.put("journal.source.JJFSE", "LOAFD7002851000012202306s.dat");
        p.proc(context, "config.yml");
    }
}
