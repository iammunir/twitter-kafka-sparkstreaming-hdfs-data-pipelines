package com.github.iammunir.consumer;

import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Java Bean class for converting RDD to DataFrame
 */
public class JavaRowClean implements java.io.Serializable {
    private String tweet_id;
    private String timestamp_ms;
    private String device;
    private String user;
    private String user_id;
    private int follower;
    private String location;
    private String language;
    private String text_status;
    private String url;
    private String main_title;
    private String song_title;
    private String artist;
    private String type;
    private String description;
    private int duration;

    public void transformTweet(String dataText) {
        String tweetId = null;
        String timestamp = null;
        String device = null;
        String language = null;

        String user = null;
        String userId = null;
        int follower = 0;
        String location = null;
        String textStatus = null;
        String extractedUrl = null;
        Map<String, String> dataFromSpotify = new HashMap<>();

        try {
            extractedUrl = extractUrl(dataText);
            if (extractedUrl != null) {
                dataFromSpotify = getDataFromSpotify(extractedUrl);
            }

            JSONObject obj = new JSONObject(dataText);
            tweetId = obj.getString("id_str");
            language = obj.getString("lang");
            textStatus = obj.getString("text");
            timestamp = obj.getString("timestamp_ms");

            String sourceTag = obj.getString("source");
            device = extractSource(sourceTag);

            JSONObject userDetail = obj.getJSONObject("user");
            user = userDetail.getString("screen_name");
            userId = userDetail.getString("id_str");
            follower = userDetail.getInt("followers_count");
            location = userDetail.getString("location");

        } catch (JSONException ignored) { }

        setTweet_id(tweetId);
        setTimestamp_ms(timestamp);
        setDevice(device);
        setUser(user);
        setUser_id(userId);
        setFollower(follower);
        setLocation(location);
        setLanguage(language);
        setText_status(textStatus);
        setUrl(extractedUrl);
        setMain_title(dataFromSpotify.get("mainTitle"));
        setSong_title(dataFromSpotify.get("songTitle"));
        setArtist(dataFromSpotify.get("artist"));
        setType(dataFromSpotify.get("type"));
        setDescription(dataFromSpotify.get("description"));
        try {
                setDuration(
                        Integer.parseInt(dataFromSpotify.get("duration"))
                );
        } catch (Exception ignored) {}
    }

    public String extractSource(String tag) {
        String source = null;
        try {
            source = tag.substring(tag.indexOf("\">") + 2, tag.indexOf("</a"));
        } catch (Exception e) {
            return null;
        }
        return source;
    }

    public String extractUrl(String json) {
        Pattern pattern = Pattern.compile("(open.spotify.com)(.*?)(\")");
        Matcher matcher = pattern.matcher(json);

        List<String> listMatches = new ArrayList<>();

        while(matcher.find()) {
            String url = (matcher.group(1) + matcher.group(2)).replace("\\", "");
            listMatches.add(url);
        }

        if (listMatches.size() > 0) {
            return getLongestString(listMatches);
        } else {
            return null;
        }
    }

    public String getLongestString(List<String> array) {
        int maxLength = 0;
        String longestString = null;
        for (String s : array) {
            if (s.length() > maxLength) {
                maxLength = s.length();
                longestString = s;
            }
        }
        return "https://"+longestString;
    }

    public Map<String, String> getDataFromSpotify(String url) {
        Map<String, String> detail = new HashMap<>();
        String mainTitle = null;
        String songTitle = null;
        String artist = null;
        String type = null;
        String description = null;
        String duration = null;
        try {
            Document doc = Jsoup.connect(url).get();
            mainTitle = doc.title();
            songTitle = extractMetaValue(doc, "og:title");
            artist = extractMetaValue(doc, "twitter:audio:artist_name");
            type = extractMetaValue(doc, "og:type");
            description = extractMetaValue(doc, "og:description");
            duration = extractMetaValue(doc, "music:duration");
        } catch (IOException ignored) {}
        detail.put("mainTitle", mainTitle);
        detail.put("songTitle", songTitle);
        detail.put("artist", artist);
        detail.put("type", type);
        detail.put("description", description);
        detail.put("duration", duration);

        return detail;
    }

    public String extractMetaValue(Document docoment, String property) {
        String result = null;
        try {
            Elements metalinks = docoment.select("meta[property=" + property + "]");
            result = metalinks.first().attr("content");
        } catch (Exception e) {
            return result;
        }
        return result;
    }

    public String getTweet_id() {
        return tweet_id;
    }

    public void setTweet_id(String tweet_id) {
        this.tweet_id = tweet_id;
    }

    public String getTimestamp_ms() {
        return timestamp_ms;
    }

    public void setTimestamp_ms(String timestamp_ms) {
        this.timestamp_ms = timestamp_ms;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public int getFollower() {
        return follower;
    }

    public void setFollower(int follower) {
        this.follower = follower;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getText_status() {
        return text_status;
    }

    public void setText_status(String text_status) {
        this.text_status = text_status;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMain_title() {
        return main_title;
    }

    public void setMain_title(String main_title) {
        this.main_title = main_title;
    }

    public String getSong_title() {
        return song_title;
    }

    public void setSong_title(String song_title) {
        this.song_title = song_title;
    }

    public String getArtist() {
        return artist;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }
}
