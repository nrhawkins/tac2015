package edu.knowitall.tac2013.stanford.annotator.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * utility functions for serialization
 * @author xiaoling
 *
 */
public class Serializer {
  //private final static Logger logger = LoggerFactory.getLogger(Serializer.class);

  public synchronized static void serialize(Object obj, String filename) {
    FileOutputStream fos = null;
    ObjectOutputStream out = null;
    try {
      fos = new FileOutputStream(filename);
      out = new ObjectOutputStream(fos);
      out.writeObject(obj);
      out.close();
    } catch (IOException ex) {
      //logger.error("failing to save the object to {}", filename);
      //logger.error(ex.getMessage(), ex);
    }
  }

  public static Object deserialize(InputStream stream) {
    ObjectInputStream in = null;
    Object obj = null;
    try {
      in = new ObjectInputStream(stream);
      obj = in.readObject();
      in.close();
    } catch (Exception ex) {
      //logger.error(ex.getMessage(), ex);
    }
    return obj;
  }

  public static Object deserialize(String filename) {
    FileInputStream fis = null;
    ObjectInputStream in = null;
    Object obj = null;
    try {
      fis = new FileInputStream(filename);
      in = new ObjectInputStream(fis);
      obj = in.readObject();
      in.close();
    } catch (Exception ex) {
      //logger.error("failing to load the object from {}", filename);
      //logger.error(ex.getMessage(), ex);
    }
    return obj;
  }
}
