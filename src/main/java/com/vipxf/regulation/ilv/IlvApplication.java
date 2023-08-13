package com.vipxf.regulation.ilv;

import com.vipxf.regulation.ilv.core.YamlProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IlvApplication {

    public static void main(String[] args) {
        SpringApplication.run(IlvApplication.class, args);

        YamlProcessor p = new YamlProcessor();
        p.proc("config.yml");
    }
}
