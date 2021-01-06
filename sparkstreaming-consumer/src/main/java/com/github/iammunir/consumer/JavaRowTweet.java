package com.github.iammunir.consumer;

/**
 * Java Bean class for converting RDD to DataFrame
 */
public class JavaRowTweet implements java.io.Serializable {
    private String tweet;

    public String getTweet() {
        return tweet;
    }

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }
}

