/* Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.happyblueduck.lembas.datastore;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.common.collect.Lists;
import com.happyblueduck.lembas.core.LembasObject;
import com.happyblueduck.lembas.core.LembasUtil;
import com.happyblueduck.lembas.core.UtilSerializeException;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Base entity class for NoSQL.
 *
 * @author Michael Tang (ntang@google.com)
 */
public class HandsomeEntity extends LembasObject implements Serializable {
    protected Entity entity;

    //private static final long serialVersionUID = 112671230986712376L;

    // GENERIC STATUS FOR ENTITIES
    public static final int INACTIVE       = 0;
    public static final int ACTIVE         = 1;
    public static final int ARCHIVED       = 2;
    public static final int SUSPENDED      = 3;
    public int _status;

    public void activate(){
        _status = ACTIVE;
    }

    public void deactivate(){
        _status = INACTIVE;
    }

    public int getStatus() {
        return _status;
    }

    public void setStatus(Long l){
        setStatus(l.intValue());
    }

    public void setStatus(int s){
        _status = s;
    }

    public void set_status(int s){
        _status = s;
    }


    public HandsomeEntity() {
        this.objectKey =  UUID.randomUUID().toString();
//        Key key = KeyFactory.createKey(getClassName(), this.objectKey);
//        this.entity = new Entity(key);
    }

    public HandsomeEntity(String keyName){
        this.entity = new Entity(this.getClass().getSimpleName(), keyName);
        this.objectKey =  KeyFactory.keyToString(entity.getKey());
    }


    public HandsomeEntity(String keyName, Key parentKey){
        this.entity = new Entity(this.getClass().getSimpleName(), keyName, parentKey);
        this.objectKey =  KeyFactory.keyToString(entity.getKey());
    }


    public void copy(HandsomeEntity that){

        ArrayList<Field> fields = Lists.newArrayList(that.getClass().getFields());
        for (Field f :fields){
            try {

                int modifiers = f.getModifiers();
                if (Modifier.isPrivate(modifiers)) continue;
                if (Modifier.isStatic(modifiers)) continue;
                if (Modifier.isTransient(modifiers)) continue;
                if (Modifier.isFinal(modifiers)) continue;
                if (Modifier.isVolatile(modifiers)) continue;

                if (f.getName().equalsIgnoreCase("objectKey"))
                    continue;

                Object value = f.get(that);
                if ( value != null)
                    this.setField(f, value);

            }catch (IllegalAccessException exception){
                exception.printStackTrace();
            }
        }
    }


    public void setField(Field f, Object value ) throws IllegalAccessException {
        //f.set(this, value);
        super.setField(f, value);
        Object _value = value;

        if ( this.entity != null){
            if ( f.getType().isEnum()){

              _value = ((Enum)value).ordinal();
            }

            if ( this.entity.getProperty(f.getName()) != _value)
                this.entity.setProperty(f.getName(), _value);

        }
    }


    public void readField(Field f, Object value) throws IllegalAccessException {
        if ( f.getType().isEnum()){

            Class z = f.getType();
            Object[] cons = z.getEnumConstants();
            int intValue = -1;
            if ( value instanceof Long)
                intValue = ((Long)value).intValue();

            for (int i = 0; i < cons.length; i++) {
                if (i == intValue) {
                    f.set(this, Enum.valueOf((Class<Enum>) f.getType(), cons[i].toString()));
                }
            }
        } else {
            f.set(this, value);
        }
    }

    /**
     * writes changes to entity
     */
    public void write(){
        copy(this);
    }

    protected HandsomeEntity(Entity entity) {
        super();
        this.setEntity(entity);
    }


    public boolean setupFieldWithMehod(String fieldName, Object value, Method[] methods) throws InvocationTargetException, IllegalAccessException {
        boolean consumed = false;
        // look for a setter method
        String setterName= fieldName;
        //underscore is used for inner fields!
        if ( setterName.startsWith("_"))
            setterName = fieldName.substring(1);
        setterName = "set" + setterName;

        for ( Method m : methods) {
            if ( m.getName().equalsIgnoreCase(setterName)){
                if ( m.getParameterTypes().length >0 ) {
                    if (m.getParameterTypes()[0].equals(value.getClass())) {

                        m.invoke(this, value);
                        consumed = true;
                        break;
                    }
                }
            }
        }

        return consumed;
    }

    /**
     * Builds lembas object from entity, setup properties
     * @param entity
     */
    protected void setEntity(Entity entity){

        this.entity = entity;
        //this.objectKey = entity.getKey().getName();
        this.objectKey = KeyFactory.keyToString(entity.getKey());

        Method[] methods = this.getClass().getMethods();

        for (String fieldName : this.entity.getProperties().keySet()){
            try {
                Object value = this.entity.getProperty(fieldName);
                Field f = this.getClass().getField(fieldName);
                //Class c = f.getClass();

                boolean consumed = setupFieldWithMehod(fieldName, value, methods);

                if (!consumed) {
                    //f.set(this, value);
                    readField(f,value);
                }


            } catch (NoSuchFieldException e) {
                logger.info("no such field, will skip:"+fieldName);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    public Entity getEntity() {
        return entity;
    }




    public Boolean setProperty(String fieldName, Object value){

        if ( entity == null) // premature property setting
            return false;

        try {
            Field f = this.getClass().getField(fieldName);
            if ( f != null){
                f.set(this,value);
            }
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        this.entity.setProperty(fieldName, value);
        return true;
    }

    public String getObjectKey(){
        return objectKey;
    }

    public Key getKey(){
        //return this.entity.getKey();
        return KeyFactory.stringToKey(this.objectKey);
    }

    public Key getParent(){
        return this.entity.getParent();
    }

    public JSONValue toJSON() {
        return null;
    }

    public Key getParentKey(){
        Key key = this.getKey();
        return key.getParent();
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {

        try {
            org.json.simple.JSONObject result = LembasUtil.serialize(this, false);
            String jsonString = result.toJSONString();
            out.write(jsonString.getBytes());
            //logger.info("> serializing"+ this.getClassName() +":"+ jsonString);
        } catch (UtilSerializeException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            throw new IOException();

        }

    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException, UtilSerializeException {

        InputStreamReader isr = new InputStreamReader(in, "UTF8");
        JSONObject object = (JSONObject) JSONValue.parse(isr);
        HandsomeEntity shallow = (HandsomeEntity) LembasUtil.deserialize(object);
        this.copy(shallow);
        this.objectKey = shallow.objectKey;
        //logger.info("< deserializing "+this.getClassName() +":" + object.toJSONString());
    }

    private void readObjectNoData()
            throws ObjectStreamException {
        System.out.println("reached");
    }
}
