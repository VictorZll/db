package com.newroad.entity;

import java.io.Serializable;

public class User implements Serializable {
private String uploader;
private Integer videos;
private Integer friends;

public User() {
	super();
}
public User(String uploader, Integer videos, Integer friends) {
	super();
	this.uploader = uploader;
	this.videos = videos;
	this.friends = friends;
}
public String getUploader() {
	return uploader;
}
public void setUploader(String uploader) {
	this.uploader = uploader;
}
public Integer getVideos() {
	return videos;
}
public void setVideos(Integer videos) {
	this.videos = videos;
}
@Override
public String toString() {
	//return String.format("%s,%s,%s", this.uploader,this.friends,this.videos);
return "User [uploader=" + uploader + ", videos=" + videos + ", friends=" + friends + "]";
}
public Integer getFriends() {
	return friends;
}
public void setFriends(Integer friends) {
	this.friends = friends;
}

}
