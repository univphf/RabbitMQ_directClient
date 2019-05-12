package com.ht.dev.directc;

import com.rabbitmq.client.*;
import java.io.IOException;

public class ReceiveLogsDirect {

  private static final String EXCHANGE_NAME = "direct_logs";

  public static void main(String[] argv) throws Exception
  {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
    String queueName = channel.queueDeclare().getQueue();

    if (argv.length < 1)
    {
      System.err.println("Syntaxe: ReceiveLogsDirect [blue] [green] [red]");
      System.exit(1);
    }

    for(String color : argv)
    {
      channel.queueBind(queueName, EXCHANGE_NAME, color);
        System.out.println("a l'ecoute de : "+color);
    }
    System.out.println(" [*] En attente de messages. CTRL+C pour quitter");

    Consumer consumer = new DefaultConsumer(channel)
    {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,AMQP.BasicProperties properties, byte[] body) throws IOException
      {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] reception de '" + envelope.getRoutingKey() + "':'" + message + "'");
          System.out.println("Nom de la Queue : "+queueName);
      }
    };

    channel.basicConsume(queueName, true, consumer);
  }

}

