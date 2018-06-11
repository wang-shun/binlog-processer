package com.datatrees.datacenter.schema.service;

import com.datatrees.datacenter.schema.service.loader.HistoryLoader;
import com.datatrees.datacenter.schema.service.repository.SchemaRepository;
import com.datatrees.datacenter.schema.service.rpc.RpcService;
import io.debezium.config.Configuration;

public class Main {

    public static void main(String[] args) {

        try {
            Configuration configuration = Configuration.create().build();

            SchemaRepository schemaRepository = new SchemaRepository(configuration);

            HistoryLoader loader = new HistoryLoader(schemaRepository);
            loader.init();
            loader.start();

            RpcService rpcService = new RpcService();
            rpcService.init();
            rpcService.start();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("application failed");
            System.exit(-1);
        }

    }
}
