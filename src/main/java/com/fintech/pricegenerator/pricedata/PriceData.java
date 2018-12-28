package com.fintech.pricegenerator.pricedata;

import org.springframework.jms.UncategorizedJmsException;
import org.springframework.jms.core.JmsTemplate;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import javax.jms.Queue;

public class PriceData implements Callable {

    private List<String> prices;
    private String stockName;
    private String stockSymbol;
    private JmsTemplate jmsTemplate;
    private Queue queueName;
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private static Logger log  = Logger.getLogger(PriceData.class);

    public PriceData(String stockSymbol, List<String> prices, JmsTemplate jmsTemplate, Queue queueName) {
        this.prices = Collections.unmodifiableList(prices);
        this.stockSymbol = stockSymbol;
        this.jmsTemplate = jmsTemplate;
        this.queueName = queueName;
    }

    @Override
    public Object call() throws InterruptedException {
        while(true){
            for (String priceData : this.prices){
                String[] data = priceData.split("\"");
                String timestamp = data[1];
                String open = data[3];
                String high = data [5];
                String low = data [7];
                String close = data [9];
                String volume = data [11];

                // Spring will convert a HashMap into a MapMessage using the default MessageConverter.
                HashMap<String, String> priceMessage = new HashMap<>();
                priceMessage.put("timestamp", timestamp);
                priceMessage.put("open", open);
                priceMessage.put("high", high);
                priceMessage.put("low", low);
                priceMessage.put("close", close);
                priceMessage.put("volume", volume);

                sendToQueue(priceMessage);

                // We have an element of randomness to help the queue be nicely
                // distributed
                // delay(Math.random() * 10000 + 2000);
            }
        }
    }

    /**
     * Sends a message to the price queue - we've hardcoded this in at present - of course
     * this needs to be fixed on the course!
     * @param priceMessage
     * @throws InterruptedException
     */
    private void sendToQueue(Map<String, String> priceMessage) throws InterruptedException {
        boolean messageNotSent = true;
        while(messageNotSent) {
            // broadcast this report
            try {
//                System.out.println("Posting a price-message to the queue.");
//                System.out.println(priceMessage);
                jmsTemplate.convertAndSend(queueName, priceMessage);
                messageNotSent = false;
            }catch (UncategorizedJmsException e) {
                // we are going to assume that this is due to downtime - back off and try again
                log.warn("Queue unavailable - backing off 2000ms before retry");
                delay(2000);
            }
        }
    }

    private static void delay(double d) throws InterruptedException {
        log.debug("Sleeping for " + d + " millsecs");
        Thread.sleep((long) d);
    }
}
