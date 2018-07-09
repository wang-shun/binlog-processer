package com.datatrees.datacenter.table.service;

import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.table.service.loader.HistoryLoader;
import com.datatrees.datacenter.table.service.repository.SchemaRepository;
import com.datatrees.datacenter.table.service.rpc.RpcService;

public class BinlogService implements TaskRunner {

    @Override
    public void process() {
        try {

            SchemaRepository schemaRepository = new SchemaRepository();
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
