package cn.learn.storm.spouts;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private boolean completed = false;
//	private FileReader fileReader;
	BufferedReader bufferReader;
	
	//当一个tuple处理成功时，会执行此方法
	@Override
	public void ack(Object msgId) {
		System. out.println("WordReader.ack(Object msgId):" + msgId);
	}

	

	//当一个tuple处理失败时，会执行此方法
	@Override
	public void fail(Object msgId) {
		System.out.println("WordReader.fail(Object msgId)" + msgId);
	}

	//只负责发送数据，这个方法会不断的被调用，直到整个文件都读完了，我们将等待并返回。
	@Override
	public void nextTuple() {
		System.out.println("WordReader.nextTuple()");
		if(completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				
			}
			return;
		}
		String str;
//		bufferReader = new BufferedReader(fileReader);
		try {
			int i = 0;
			while((str = bufferReader.readLine())!=null) {
				System.out.println("WordReader.nextTuple(),emits time:" + i++);
				//按行发布一个新值
				this.collector.emit(new Values( str), str );
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}
	}

	//当spout被创建时，这个方法会被调用
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		System.out.println("WordReader.open(Map conf, TopologyContext context, SpoutOutputCollector collector)");
		String fileName = conf.get("fileName").toString();
		InputStream inputStream = WordReader.class.getClassLoader().getResourceAsStream(fileName);
		bufferReader = new BufferedReader(new InputStreamReader(inputStream));
		this.collector = collector;

	}

	//声明数据格式，即输出的一个Tuple中，包含几个字段
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("WordReader.declareOutputFields(OutputFieldDeclarer)");
		declarer.declare(new Fields("line"));
	}

	//
	@Override
	public void activate() {
		System.out.println("WordReader.activate() ");

	}

	//当一个topology结束时，会执行此方法
	@Override
	public void close() {
		System.out.println("WordReader.close() ");
	}

	@Override
	public void deactivate() {
		System.out.println("WordReader.deactivate() ");
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		System.out.println("WordReader.getComponentConfiguration() ");
		return null;
	}

}

//package com.tianshouzhi.study.wordcountapp.spouts;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.util.Map;
// 
//import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.IRichSpout;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
// 
///**
// * WordReader(Spout)，用于从外部数据源words.txt中获取数据
// */
//public class WordReader implements IRichSpout {
//       private SpoutOutputCollector collector ;
//       private FileReader fileReader ;
//       BufferedReader reader;
//       private boolean completed = false;
// 
//       /**
//       * 这个方法做的惟一一件事情就是分发文件中的文本行
//       */
//       public void nextTuple() {
//             /**
//             * 这个方法会不断的被调用，直到整个文件都读完了，我们将等待并返回。
//             */
//             if (completed ) {
//                   try {
//                        Thread. sleep(1000);
//                  } catch (InterruptedException e ) {
//                         // 什么也不做
//                  }
//                   return;
//            }
//            String str;
//             
//             try {
//                   int i = 0;
//                   // 读所有文本行
//                   while ((str = reader.readLine()) != null) {
//                        System. out.println("WordReader.nextTuple(),emits time:" + i++);
//                         /**
//                         * 按行发布一个新值
//                         */
//                         this.collector .emit(new Values( str), str );
//                  }
//            } catch (Exception e ) {
//                   throw new RuntimeException("Error reading tuple", e);
//            } finally {
//                   completed = true ;
//            }
//      }
// 
//       /**
//       *
//       * 当Spout被创建之后，这个方法会被条用
//       */
//       public void open(Map conf, TopologyContext context , SpoutOutputCollector collector ) {
//      
//    	   System.out.println( "WordReader.open(Map conf, TopologyContext context, SpoutOutputCollector collector)");
//            String fileName = conf .get("fileName").toString();
//            InputStream inputStream=WordReader.class.getClassLoader().getResourceAsStream( fileName);
//             reader =new BufferedReader(new InputStreamReader(inputStream ));
//             this.collector = collector;
//      }
// 
//       /**
//       * 声明数据格式，即输出的一个Tuple中，包含几个字段
//       */
//       public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            System. out.println("WordReader.declareOutputFields(OutputFieldsDeclarer declarer)");
//             declarer.declare(new Fields("line"));
//      }
// 
//       @Override
//       public void activate() {
//            System. out.println("WordReader.activate()" );
//      }
// 
//       @Override
//       public void deactivate() {
//            System. out.println("WordReader.deactivate()" );
//      }
// 
//       @Override
//       public Map<String, Object> getComponentConfiguration() {
//            System. out.println("WordReader.getComponentConfiguration()" );
//             return null ;
//      }
// 
//       /**
//       * 当一个Tuple处理成功时，会调用这个方法
//       */
//       public void ack(Object msgId) {
//            System. out.println("WordReader.ack(Object msgId):" + msgId);
//      }
// 
//       /**
//       * 当Topology停止时，会调用这个方法
//       */
//       public void close() {
//            System. out.println("WordReader.close()" );
//      }
// 
//       /**
//       * 当一个Tuple处理失败时，会调用这个方法
//       */
//       public void fail(Object msgId) {
//            System. out.println("WordReader.fail(Object msgId):" + msgId);
//      }
//}
