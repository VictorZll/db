package com.newroad.entity;

import java.io.Serializable;

public class Keys implements Serializable {
	private Integer videos;
	private Integer friends;
	public Integer getVideos() {
		return videos;
	}
	public void setVideos(Integer videos) {
		this.videos = videos;
	}
	public Integer getFriends() {
		return friends;
	}
	public void setFriends(Integer friends) {
		this.friends = friends;
	}
	
	public Keys() {
		super();
	}
	public Keys(Integer videos, Integer friends) {
		super();
		this.videos = videos;
		this.friends = friends;
	}
	@Override
	public String toString() {
		return "Keys [videos=" + videos + ", friends=" + friends + "]";
	}
	

}
