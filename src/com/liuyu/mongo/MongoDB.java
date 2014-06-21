package com.liuyu.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
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
 * @date 2014-5-29 下午5:22:51 
 *    
 */
public class MongoDB {
	//1.建立Mongo数据库对象
	static Mongo connection = null; 
	//2.创建相关数据库连接
	static DB db = null;
	
	public MongoDB(String databaseName) throws UnknownHostException, MongoException{
		connection = new Mongo("127.0.0.1:27017");
		db = connection.getDB(databaseName);
	}
	
	public static void main(String[] args) throws UnknownHostException, MongoException {
		MongoDB mongoDb = new MongoDB("foobar");
		
		/**
		 * 1.创建名字叫javadb的数据库
		 */
//		mongoDb.createCollection("javadb");
		/**
		 * 2.为集合javadb添加一条记录
		 */
//		DBObject dbs = new BasicDBObject();
//		dbs.put("name", "tom");
//		dbs.put("age", 22);
//		List<String> books = new ArrayList<String>();
//		books.add("MONGODB");
//		books.add("EXTJS");
//		dbs.put("books", books);
//		mongoDb.insert(dbs,"javadb");
		/**
		 * 3.批量插入多条数据
		 */
//		List<DBObject> dbObjects = new ArrayList<DBObject>();
//		DBObject jim = new BasicDBObject("name","jim");
//		jim.put("age", 21);
//		DBObject lisi = new BasicDBObject("name","lisi");
//		lisi.put("age", 23);
//		dbObjects.add(jim);
//		dbObjects.add(lisi);
//		mongoDb.insertBatch(dbObjects, "javadb");
		/**
		 * 4.通过ID删除一条记录
		 */
//		System.out.println(mongoDb.deleteById("53870284d2625a16eb1a0d70", "javadb"));
		/**
		 * 5.根据条件删除记录
		 */
//		DBObject dbs = new BasicDBObject();
//		dbs.put("name", "jim");
//		mongoDb.deleteByDbs(dbs, "javadb");
		/**
		 * 6.更新操作，
		 */
//		DBObject update = new BasicDBObject();
//		update.put("$set", new BasicDBObject("email","281541534@qq.ocm"));
//		mongoDb.update(new BasicDBObject(),update,false,true,"javadb");
		/**
		 * 7.查询出persons集合中的name和age
		 */
//		DBObject keys = new BasicDBObject();
//		keys.put("_id", false);
//		keys.put("name", true);
//		keys.put("age", true);
//		DBCursor cursor = mongoDb.find(null,keys,"javadb");
//		while(cursor.hasNext()){
//			DBObject object = cursor.next();
//			System.out.println(object.get("name")+":"+object.get("age"));
//		}
		
		/**
		 * 8.查询出年龄大于26岁并且英语成绩小于80分
		 * new BasicDBObject();就相当于shell中的{key:value}
		 */
//		DBObject ref = new BasicDBObject();
//		ref.put("age", new BasicDBObject("$gte",26));
//		ref.put("e", new BasicDBObject("$lte",80));
//		DBCursor cursor = mongoDb.find(ref, null, "persons");
//		while(cursor.hasNext()){
//			DBObject object = cursor.next();
//			System.out.println(object.get("name")+":"+object.get("age")
//					+":"+object.get("e"));
//		}
		/**
		 * 9.分页查询
		 */
		DBCursor cursor = mongoDb.find(null, null, 10, 3, "persons");
		while(cursor.hasNext()){
			DBObject object = cursor.next();
			System.out.println(object.get("name")+":"+object.get("age")
					+":"+object.get("e"));
		}
		
		connection.close();
	}
	

	/**
	 * 创建一个数据库集合
	 * @param collName
	 */
	public void createCollection(String collName){
		DBObject dbs = new BasicDBObject();
		db.createCollection(collName, dbs);
	}
	
	/**
	 * 添加一条数据
	 * @param dbs
	 * @param collName
	 */
	public void insert(DBObject dbs, String collName) {
		//获取集合
		DBCollection coll = db.getCollection(collName);
		coll.insert(dbs);
	}
	
	/**
	 * 批量插入多条数据
	 * @param dbObjects
	 * @param collName
	 */
	public void insertBatch(List<DBObject> dbObjects, String collName){
		DBCollection coll = db.getCollection(collName);
		coll.insert(dbObjects);
	}
	
	/**
	 * 根据ID删除数据
	 * @param id
	 * @param collName
	 * @return
	 */
	public int deleteById(String id, String collName){
		DBCollection coll = db.getCollection(collName);
		DBObject dbs = new BasicDBObject("_id",new ObjectId(id));
		int count = coll.remove(dbs).getN();
		return count;
	}
	
	/**
	 * 根据条件删除记录
	 * @param dbs
	 * @param collName
	 * @return
	 */
	public int deleteByDbs(DBObject dbs, String collName){
		DBCollection coll = db.getCollection(collName);
		int count = coll.remove(dbs).getN();
		return count;
	}
	
	/**
	 * 更新记录
	 * @param find  查询器
	 * @param update  更新器
	 * @param upsert  更新或插入
	 * @param multi  是否批量更新
	 * @param collName 集合名称
	 * @return  返回影响的数据条数
	 */
	public int update(DBObject find,DBObject update,boolean upsert, boolean multi, String collName){
		DBCollection coll = db.getCollection(collName);
		int count = coll.update(find, update, upsert, multi).getN();
		return count;
	}
	
	/**
	 * 查询器（不分页）
	 * @param ref
	 * @param keys
	 * @param collName
	 * @return
	 */
	public DBCursor find(DBObject ref, DBObject keys, String collName){
		DBCollection coll = db.getCollection(collName);
		DBCursor cur = coll.find(ref,keys);
		return cur;
	}
	
	/**
	 * 查询器（分页）
	 * @param ref
	 * @param keys
	 * @param start
	 * @param limit
	 * @param collName
	 * @return
	 */
	public DBCursor find(DBObject ref,DBObject keys,int start, int limit,String collName){
		DBCursor cur = find(ref,keys,collName);
		return cur.limit(limit).skip(start);
	}
	
	
	
	
}


