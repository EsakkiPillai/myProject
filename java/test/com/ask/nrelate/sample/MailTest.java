/*
package com.ask.nrelate.sample;

*/
/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 9/10/13
 * Time: 12:28 PM
 * To change this template use File | Settings | File Templates.
 *//*

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class MailTest {

    public static void main(String[] args) {

        final String username = "kaniyarasu@gmail.com";
        final String password = "ccccc";

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password);
                    }
                });

        try {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress("kaniyarasu@gmail.com"));
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse("kaniyarasu@gmail.com"));
            message.setSubject("Testing Subject");
            message.setText("Dear Mail Crawler,"
                    + "\n\n No spam to my email, please!");

            Transport.send(message);

            System.out.println("Done");

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }
}*/
