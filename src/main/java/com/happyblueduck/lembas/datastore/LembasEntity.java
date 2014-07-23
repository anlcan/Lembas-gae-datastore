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
public class LembasEntity extends LembasObject implements Serializable {
    public static final String LEMBAS_PROPERTY_IDENTIFIER = "$_";
    protected Entity entity;

    //private static final long serialVersionUID = 112671230986712376L;

    // GENERIC STATUS FOR ENTITIES
    public static final int INACTIVE       = 0;
    public static final int ACTIVE         = 1;
    public static final int ARCHIVED       = 2;
    public static final int SUSPENDED      = 3;
    public static final int CANCELLED      = 3;
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


    public LembasEntity() {
        this.objectKey =  UUID.randomUUID().toString();
    }

    public LembasEntity(String keyName){
        this.entity = new Entity(getKind(), keyName);
        this.objectKey =  KeyFactory.keyToString(entity.getKey());
    }


    public LembasEntity(String keyName, Key parentKey){
        this.entity = new Entity(getKind(), keyName, parentKey);
        this.objectKey =  KeyFactory.keyToString(entity.getKey());
    }

    protected LembasEntity(Entity entity) {
        super();
        this.setEntity(entity);
    }


    public String getKind(){
        return this.getClass().getSimpleName();
    }

    /**
     * copies values from  lembasEntity to this entity. skip objectKey from that.
     * @param that
     */
    public void copy(LembasEntity that){

        ArrayList<Field> fields = Lists.newArrayList(that.getClass().getFields());
        for (Field f :fields){
            try {

                int modifiers = f.getModifiers();
                if (Modifier.isPrivate(modifiers)) continue;
                if (Modifier.isStatic(modifiers)) continue;
                if (Modifier.isTransient(modifiers)) continue;
                if (Modifier.isFinal(modifiers)) continue;
                if (Modifier.isVolatile(modifiers)) continue;

                if (f.getName().equalsIgnoreCase(LembasUtil.objectKey))
                    continue;

                Object value = f.get(that);
                if ( value != null)
                    this.setField(f, value);

            }catch (IllegalAccessException exception){
                exception.printStackTrace();
            }
        }
    }

    /**
     * sets the field name @{param:fieldName}
     * @param fieldName
     * @param value
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public void setField(String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        Field f = this.getClass().getField(fieldName);
        setField(f, value);
    }

    /**
     * sets the fields, for this object end the entity
     * @param f
     * @param value
     * @throws IllegalAccessException
     */
    public void setField(Field f, Object value ) throws IllegalAccessException {
        //f.set(this, value);
        super.setField(f, value);
        Object _value = value;

        if ( this.entity != null){
            // storing lembasEntities
            if ( LembasEntity.class.isAssignableFrom(value.getClass())){
                try {
                    String _serialized = LembasUtil.serialize(value).toJSONString();
                    this.entity.setProperty(LEMBAS_PROPERTY_IDENTIFIER + f.getName(), _serialized);

                    return;
                } catch (UtilSerializeException e) {
                    e.printStackTrace();
                }
            }

            // store enums with their ordinals
            if ( f.getType().isEnum()){
                if ( value instanceof  Number)
                    _value = value;
                else
                    _value = ((Enum)value).ordinal();
            }

            // poor man's cache
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



    public boolean setupFieldWithMethod(String fieldName, Object value, Method[] methods) throws InvocationTargetException, IllegalAccessException {
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

    public void setLembasField(Field f, Object value) throws IllegalAccessException {
        JSONObject jsonObject = (JSONObject) JSONValue.parse((String) value);
        try {
            LembasEntity obj = (LembasEntity) LembasUtil.deserialize(jsonObject);
            f.set(this, obj);
        } catch (UtilSerializeException e) {
            e.printStackTrace();
        }
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

                // reading lembas identifier
                if ( fieldName.startsWith(LEMBAS_PROPERTY_IDENTIFIER) ){
                    fieldName = fieldName.substring(2);
                    Field f = this.getClass().getField(fieldName);
                    setLembasField(f, value);
                    continue;
                }

                Field f = this.getClass().getField(fieldName);
                boolean consumed = setupFieldWithMethod(fieldName, value, methods);

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
            //e.printStackTrace();
        } catch (IllegalAccessException e) {
            //e.printStackTrace();
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

    public String toJSON() {
        try {
            return LembasUtil.serialize(this, false).toJSONString();
        } catch (UtilSerializeException e) {
            e.printStackTrace();
            return null;
        }
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
        LembasEntity shallow = (LembasEntity) LembasUtil.deserialize(object);
        this.copy(shallow);
        this.objectKey = shallow.objectKey;
        //logger.info("< deserializing "+this.getClassName() +":" + object.toJSONString());
    }

    private void readObjectNoData()
            throws ObjectStreamException {
        System.out.println("reached");
    }
}
