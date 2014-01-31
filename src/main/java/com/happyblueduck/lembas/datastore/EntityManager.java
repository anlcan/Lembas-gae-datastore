/*
 * Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.happyblueduck.lembas.datastore;

import com.google.appengine.api.datastore.*;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.*;

import static com.google.appengine.api.datastore.DatastoreServiceFactory.getDatastoreService;

/**
 * Base class for entity managers for NoSQL implementation.
 *
 * @author Michael Tang (ntang@google.com)
 *
 * @param <T> type extends {@code LembasEntity}
 */
public class EntityManager<T extends LembasEntity> {
    private static final Logger logger =
            Logger.getLogger(EntityManager.class.getCanonicalName());

    protected  Class<T> entityClass;

    //public boolean useMemcache = true;

    public EntityManager(Class<T> entityClass) {
        this.entityClass = entityClass;
    }

    public T deleteEntity(String entityKey) {
        return deleteEntity(getEntity(entityKey));
    }


    public T deleteEntity(T handsomeEntity) {
        Utils.assertTrue(handsomeEntity != null, "entity cannot be null");
        LembasEntity entityNoSql = downCastEntity(handsomeEntity);
        DatastoreService ds = getDatastoreService();

        Transaction txn = ds.beginTransaction();
        try {
            if (checkEntityForDelete(ds, entityNoSql)) {
                ds.delete(entityNoSql.getKey());
                txn.commit();
                logger.info("entity deleted.");
                removeFromCache(handsomeEntity.objectKey);
                return handsomeEntity;
            }
        } catch (Exception e) {
            logger.error("Failed to delete entity from com.happyblueduck.lembas.datastore:" + e.getMessage());
        } finally {
            if (txn.isActive()) {
                txn.rollback();
            }
        }
        return null;
    }

    /**
     * Callback before entity is deleted. Checks if the entity exists.
     *
     * @param ds the com.happyblueduck.lembas.datastore service object.
     * @param demoEntity the entity to be deleted.
     *
     * @return true if the entity should be deleted; otherwise, false.
     */
    protected boolean checkEntityForDelete(DatastoreService ds, LembasEntity demoEntity) {
        if (demoEntity != null) {
//            Entity entity = demoEntity.getEntity();
//            if (entity != null) {
            return getDatastoreEntity(ds, demoEntity.getKey()) != null;
//            }
        }
        return false;
    }

    private void initEntity(T handsomeEntity){
        DatastoreService ds = getDatastoreService();
        Transaction txn = ds.beginTransaction();

        try {

            Entity entity = getDatastoreEntity(ds, KeyFactory.stringToKey(handsomeEntity.objectKey));
            handsomeEntity.entity = entity;
        }
        finally {
            if (txn.isActive()) {
                txn.rollback();
            }
        }

    }

    public T upsertEntity(T handsomeEntity) {
        Utils.assertTrue(handsomeEntity != null, "handsomeEntity cannot be null");
        Utils.assertTrue(handsomeEntity.getClass().getSimpleName().equalsIgnoreCase(getKind()), "cannot insert different class");

        if ( handsomeEntity.entity == null){
            initEntity(handsomeEntity);
        }

        handsomeEntity.copy(handsomeEntity);  // FIXME:apply changes on the object to underlying entity
        DatastoreService ds = getDatastoreService();
        LembasEntity entityNoSql = downCastEntity(handsomeEntity);
        Entity entity = entityNoSql.getEntity();
        ds.put(entity);

        storeEntityToCache(handsomeEntity);
        return handsomeEntity;
    }

    /**
     * Gets the entity class.
     *
     * @return entity class.
     */
    protected Class<T> getEntityClass() {
        return this.entityClass;
    }

    /**
     * Gets the entity kind as a string.
     *
     * @return the entity kind string.
     */
    public String getKind() {
        return entityClass.getSimpleName();
        //return entityClass.getName();
    }


    public MemcacheService getCacheService(){
        return MemcacheServiceFactory.getMemcacheService(getKind());
    }

    public T getEntityFromCache(String key) {
        return (T) getCacheService().get(key);
    }

    public void storeEntityToCache(LembasEntity entity){
        if ( entity.objectKey == null){
            return;
        }
        getCacheService().put(entity.objectKey, entity);
    }

    public void removeFromCache(String key){
        getCacheService().delete(key);
    }


    /**
     * Looks up a  entity by key.
     *
     * @param key the entity key.
     * @return the demo entity; null if the key could not be found.
     */
    public T getEntity(Key key) {
        if ( getEntityFromCache(KeyFactory.keyToString(key)) != null) {
            T t =getEntityFromCache(KeyFactory.keyToString(key));
            return t;
        }

        DatastoreService ds = getDatastoreService();
        Entity entity = getDatastoreEntity(ds, key);
        if (entity != null) {
            return fromEntity(entity);
        }
        return null;
    }

    public T getEntity(String objectKey){
        //Key key = KeyFactory.createKey(getKind(), objectKey);
        Key key = KeyFactory.stringToKey(objectKey);
        return getEntity(key);
    }


    /**
     * Looks up an entity by key.
     *
     * @param ds the com.nomad.lembas.datastore service object.
     * @param key the entity key.
     * @return the entity; null if the key could not be found.
     */
    protected Entity getDatastoreEntity(DatastoreService ds, Key key) {
        try {
            return ds.get(key);
        } catch (EntityNotFoundException e) {
            logger.error("No entity found:" + key.toString());
        }
        return null;
    }

    public ArrayList<T> getEntities() {
//        Query query = new Query(getKind());
//        FetchOptions options = FetchOptions.Builder.withDefaults();
//        return queryEntities(query, options);
        return entitiesWithParentAndValues(null, null);
    }


    public T entityWithValue(String fieldName, Object fieldValue){
        Iterable<T> result = entitiesWithValue(fieldName, fieldValue);
        if ( result.iterator().hasNext()){
            return result.iterator().next();
        } else {
            return null;
        }
    }

    public T entityWithParent(LembasEntity parent){

        Iterable<T> result = entitiesWithParentAndValues(parent, null);
        if ( result.iterator().hasNext()){
            return result.iterator().next();
        } else {
            return null;
        }
    }

    public ArrayList<T> entitiesWithValue(String fieldName, Object fieldValue){

        HashMap<String, Object> values = new LinkedHashMap<String, Object>();
        values.put(fieldName, fieldValue);
        return entitiesWithParentAndValues(null, values);
    }

    public ArrayList<T> entitiesWithParent(LembasEntity parent){

        return entitiesWithParentAndValues(parent, null);
    }

    public ArrayList<T> entitiesWithParentWithValue(LembasEntity parent, String fieldName, Object fieldValue){
        HashMap<String, Object> values = new LinkedHashMap<String, Object>();
        values.put(fieldName, fieldValue);
        return entitiesWithParentAndValues(parent, values);
    }

    public ArrayList<T> entitiesWithParentAndValues(LembasEntity parent, Map<String, Object> values){
        return entitiesWithParentAndValuesSorted(parent, values, null);
    }

    public ArrayList<T> entitiesWithValues( Map<String, Object> values){
        return entitiesWithParentAndValuesSorted(null,values, null);
    }

    /**
     * https://developers.google.com/appengine/docs/java/datastore/queries
     * @param parent
     * @param values
     * @param sortDirectionMap
     * @return
     */
    public ArrayList<T> entitiesWithParentAndValuesSorted(LembasEntity parent, Map<String, Object> values, Map<String, Query.SortDirection> sortDirectionMap){

        Query query = query(parent);
        setFilters(query, values);

        if ( sortDirectionMap != null){

            for ( String value : sortDirectionMap.keySet()){
                query.addSort(value, sortDirectionMap.get(value));
            }
        }

        return queryEntities(query);
    }



    /**
     * Queries the com.nomad.lembas.datastore for an {@code Iterable} collection of entities.
     *
     *
     * @param query com.nomad.lembas.datastore query object.
     * @param options query options.
     *
     * @return an {@code Iterable} collection of com.nomad.lembas.datastore entities.
     */
    public ArrayList<T> queryEntities(Query query, FetchOptions options) {
        PreparedQuery preparedQuery = getDatastoreService().prepare(query);
        final Iterable<Entity> iterable = preparedQuery.asIterable(options);

          Iterable<T> iterableWrapper = new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                final Iterator<Entity> iterator = iterable.iterator();
                return new Iterator<T>() {
                    @Override
                    public void remove() {
                        iterator.remove();
                    }

                    @Override
                    public T next() {
                        return fromEntity(iterator.next());
                    }

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
                };
            }
        };
//        return iterableWrapper;

        ArrayList<T> result =  Lists.newArrayList(iterableWrapper);
        return result;
    }

    public ArrayList<T> queryEntities(Query q) {
        return queryEntities(q,  FetchOptions.Builder.withDefaults());
    }

    /** conveinence methods */
    public Query query() {

        Query query = new Query(this.getKind());
        return query;
    }

    public Query query(LembasEntity parent){

        Query query = query();

        if ( parent != null) {
            query.setAncestor(parent.getKey());
        }
        return query;
    }

    public static Query setFilters(Query query, Map<String, Object>values){
        if (values != null && values.size() > 0){
            query.setFilter(makeAndFilter(values));
        }

        return query;
    }

    public static Query.Filter makeAndFilter(Map<String, Object> values){
        if (values != null && values.size() > 0){
            ArrayList<Query.Filter> filters = new ArrayList<>();
            for ( String fieldName : values.keySet()){
                Object fieldValue = values.get(fieldName);
                Query.Filter filter =
                        new Query.FilterPredicate(fieldName,
                                Query.FilterOperator.EQUAL, fieldValue);

                filters.add(filter);
            }
            if ( filters.size() > 1) {
                Query.Filter compositeFilter =Query.CompositeFilterOperator.and(filters);
                return compositeFilter;

            } else {
                return filters.get(0);
            }

        } else {
            return null;
        }
    }

    public static void applyFilters(Query q, ArrayList<Query.Filter> filters){
        if (filters.size() == 1) {
            q.setFilter(filters.get(0));
        } else if (filters.size() > 1) {
            q.setFilter(Query.CompositeFilterOperator.and(filters));
        }
    }

    /**
     * Down casts the entity to a NoSQL base entity. The method makes sure the entity is created by
     * NoSQL com.nomad.lembas.datastore module.
     *
     * @param demoEntity the model entity.
     *
     * @return the downcast NoSQL base entity.
     */
    private LembasEntity downCastEntity(T demoEntity) {
        Utils.assertTrue(
                demoEntity instanceof LembasEntity, "entity has to be a valid NoSQL entity");
        LembasEntity entityNoSql = (LembasEntity) demoEntity;
        return entityNoSql;
    }



    /**
     * Creates a model entity based on parent key.
     *
     * @param parentKey the parent key.
     *
     * @return an model entity.
     */
    public  T fromParentKey(Key parentKey) {

        T instance = null;
        try {
            instance = this.entityClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IllegalAccessException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        instance.entity = new Entity(getKind(), UUID.randomUUID().toString(), parentKey);
        instance.objectKey = KeyFactory.keyToString(instance.entity.getKey());

        return instance;
    };

    public T fromParentKey(String keyString){
        Key key = KeyFactory.stringToKey(keyString);
        return  fromParentKey(key);
    }

    /**
     * Creates a model the entity based on com.nomad.lembas.datastore entity.
     *
     * @param entity com.nomad.lembas.datastore entity.
     *
     * @return an model entity.
     */
    public  T fromEntity(Entity entity)  {
        T instance = null;
        try {
            instance = this.entityClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        instance.setEntity(entity);

        storeEntityToCache(instance);
        return instance;
    };

    public T fromKey(String key){

        T instance = null;
        try {
            instance = this.entityClass.newInstance();
            instance.entity = new Entity(getKind(), key);
            instance.objectKey = KeyFactory.keyToString(instance.entity.getKey());

        } catch (InstantiationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IllegalAccessException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        storeEntityToCache(instance);
        return instance;
    }

    public T fromParentWithKey(Key parentKey, String key)  {
        T instance = null;
        try {
            instance = this.entityClass.newInstance();
            instance.entity = new Entity(getKind(), key, parentKey);
            instance.objectKey = KeyFactory.keyToString(instance.entity.getKey());

        } catch (InstantiationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IllegalAccessException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        storeEntityToCache(instance);
        return instance;
    }

    public T newEntity() {
        T instance = null;
        try {
            instance = this.entityClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        instance.entity = new Entity( KeyFactory.createKey(getKind(), instance.objectKey));
        instance.objectKey = KeyFactory.keyToString(instance.entity.getKey());

        storeEntityToCache(instance);
        return instance;
    }
}
