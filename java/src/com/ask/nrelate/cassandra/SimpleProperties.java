package com.ask.nrelate.cassandra;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 9/12/13
 * Time: 5:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleProperties {
    String userId;
    String firstName;
    String lastName;
    Set<String> emails;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Set<String> getEmails() {
        return emails;
    }

    public void setEmails(Set<String> emails) {
        this.emails = emails;
    }
    public void setEmails(String email){
        this.emails.add(email);
    }
}
