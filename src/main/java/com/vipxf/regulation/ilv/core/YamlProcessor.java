package com.vipxf.regulation.ilv.core;

import com.vipxf.regulation.ilv.core.command.Command;
import com.vipxf.regulation.ilv.core.command.CommandFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.List;

public class YamlProcessor {
    private CommandFactory commandFactory;

    public YamlProcessor() {
        this.commandFactory = new CommandFactory();
    }

    public void proc(String yamlFile) {
        YamlConfig config = load(yamlFile);
        Context context = createContext(config.workDir());
        List<Command> commands = commandFactory.createCommands(config.getSteps());
        for (Command command : commands) {
            command.execute(context);
        }
    }

    private YamlConfig load(String yamlFile) {
        Yaml yaml = new Yaml(new Constructor(YamlConfig.class, new LoaderOptions()));
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config.yml");
        return yaml.load(inputStream);
    }

    private Context createContext(String csvFile) {
        // Create a context object with methods to access and manipulate the csv data
        // Implement this according to your requirements

        return null;
    }
}

