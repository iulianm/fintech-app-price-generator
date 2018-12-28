package com.fintech.pricegenerator.pricedata;

import com.fintech.pricegenerator.PriceGeneratorApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Controller;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.Queue;

@Controller
public class PriceDataGenerator implements Runnable {

    @Autowired
    private JmsTemplate template;

    @Autowired
    private Queue queueName;

    private ExecutorService threadPool;

    @Override
    public void run() {
        try {
            this.runMarketDataSimulation();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * For each stock, a thread is started which simulates a row of market price data for that stock.
     * When all stockPrices have completed, we start all over again.
     * @throws InterruptedException
     */
    private void runMarketDataSimulation() throws InterruptedException {
        Map<String, List<String>> prices = setupData();
        threadPool = Executors.newCachedThreadPool();
        boolean stillRunning = true;
        while (stillRunning) {
            List<Callable<Object>> calls = new ArrayList<>();

            for (String stockSymbol : prices.keySet()){
                // kick off a message sending thread for this stock
                calls.add(new PriceData(stockSymbol, prices.get(stockSymbol), template, queueName));
            }

            threadPool.invokeAll(calls);
            if (threadPool.isShutdown()){
                stillRunning = false;
            }
        }
    }

    /**
     * Read the data from the resources directory - should work for an executable Jar as
     * well as through direct execution
     */
    private Map<String, List<String>> setupData() {
        Map<String, List<String>> prices = new HashMap<>();
        PathMatchingResourcePatternResolver path = new PathMatchingResourcePatternResolver();

        try {
            for (Resource nextFile : path.getResources("stockPrices/*")){
                URL resourceURL = nextFile.getURL();
                File file = new File(resourceURL.getFile());
                String stockSymbol = file.getName();
                InputStream is = PriceGeneratorApplication.class.getResourceAsStream("/stockPrices/" + file.getName());
                try(Scanner sc = new Scanner(is)){
                    List<String> thisStockPrices = new ArrayList<>();
                    while(sc.hasNextLine()){
                        String nextPrice = sc.nextLine();
                        thisStockPrices.add(nextPrice);
                    }
                    prices.put(stockSymbol,thisStockPrices);
                }
                }
            return Collections.unmodifiableMap(prices);
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    public void finish() {threadPool.shutdown();}
}











