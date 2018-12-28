package com.fintech.pricegenerator;

import com.fintech.pricegenerator.pricedata.PriceDataGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;

@SpringBootApplication
public class PriceGeneratorApplication {

    public static void main(String[] args) throws IOException, InterruptedException {
        try(ConfigurableApplicationContext ctx = SpringApplication.run(PriceGeneratorApplication.class))
        {
            final PriceDataGenerator simulator = ctx.getBean(PriceDataGenerator.class);

            Thread mainThread = new Thread(simulator);
            mainThread.start();
        }
    }
}
