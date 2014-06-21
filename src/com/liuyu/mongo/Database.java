package com.liuyu.mongo;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**   
 *  
 * @Description: 
 * @author ly   
 * @date 2014-5-29 下午4:30:13 
 *    
 */
public class Database {
	
	public static void main(String[] args) {
		try {
			//建立一个mongo的数据库连接对象
			Mongo mongo = new Mongo("127.0.0.1:27017");
			//获取数据库名称
			List<String> DBName = mongo.getDatabaseNames();
			for(String name : DBName){
				System.out.println("DBName:"+name);
			}
			//创建相关的数据库连接
			DB db = mongo.getDB("foobar");
			//获取foobar数据库中所有集合
			Set<String> collName = db.getCollectionNames();
			for(String name : collName){
				System.out.println("collName:"+name);
			}
			//查询所有数据
			DBCollection persons = db.getCollection("persons");
			//通过指针遍历数据
			DBCursor cur = persons.find();
			while(cur.hasNext()){
				DBObject object = cur.next();
				System.out.println(object.get("name"));
			}
			//获取集合中有多少条记录
			System.out.println(cur.count());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}


