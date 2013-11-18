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
package com.nomad.handsome.datastore;

import com.google.appengine.api.datastore.*;
import com.google.appengine.labs.repackaged.com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.*;

import static com.google.appengine.api.datastore.DatastoreServiceFactory.getDatastoreService;

/**
 * Base class for entity managers for NoSQL implementation.
 *
 * @author Michael Tang (ntang@google.com)
 *
 * @param <T> type extends {@code HandsomeEntity}
 */
public class EntityManager<T extends HandsomeEntity> {
    private static final Logger logger =
            Logger.getLogger(EntityManager.class.getCanonicalName());

    protected  Class<T> entityClass;

    public EntityManager(Class<T> entityClass) {
        this.entityClass = entityClass;
    }


    public T deleteEntity(String entityKey) {
        return deleteEntity(getEntity(entityKey));
    }


    public T deleteEntity(T handsomeEntity) {
        Utils.assertTrue(handsomeEntity != null, "entity cannot be null");
        HandsomeEntity entityNoSql = downCastEntity(handsomeEntity);
        DatastoreService ds = getDatastoreService();
        Transaction txn = ds.beginTransaction();
        try {
            if (checkEntityForDelete(ds, entityNoSql)) {
                ds.delete(entityNoSql.getEntity().getKey());
                txn.commit();
                logger.info("entity deleted.");
                return handsomeEntity;
            }
        } catch (Exception e) {
            logger.error("Failed to delete entity from com.nomad.handsome.datastore:" + e.getMessage());
        } finally {
            if (txn.isActive()) {
                txn.rollback();
            }
        }
        return null;
    }

    public T upsertEntity(T handsomeEntity) {
        Utils.assertTrue(handsomeEntity != null, "handsomeEntity cannot be null");
        Utils.assertTrue(handsomeEntity.getClass().getSimpleName().equalsIgnoreCase(getKind()), "cannot insert different class");

        handsomeEntity.copy(handsomeEntity);  // apply changes on the object to underlying entity
        DatastoreService ds = getDatastoreService();
        HandsomeEntity entityNoSql = downCastEntity(handsomeEntity);
        Entity entity = entityNoSql.getEntity();
        ds.put(entity);
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

    /**
     *
     * @param userId
     * @return
     */
    public Key createEntityKey(String userId) {
        return KeyFactory.createKey(getKind(), userId);
    }

    /**
     * Looks up a demo entity by key.
     *
     * @param key the entity key.
     * @return the demo entity; null if the key could not be found.
     */
    public T getEntity(Key key) {
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
     * @param ds the com.nomad.handsome.datastore service objct.
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



    public T entityWithParent(HandsomeEntity parent){

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

    public ArrayList<T> entitiesWithParent(HandsomeEntity parent){

        return entitiesWithParentAndValues(parent, null);
    }

    public ArrayList<T> entitiesWithParentWithValue(HandsomeEntity parent, String fieldName, Object fieldValue){
        HashMap<String, Object> values = new LinkedHashMap<String, Object>();
        values.put(fieldName, fieldValue);
        return entitiesWithParentAndValues(parent, values);
    }

    public ArrayList<T> entitiesWithParentAndValues(HandsomeEntity parent, Map<String, Object> values){
        return entitiesWithParentAndValuesSorted(parent,values, null);
    }

    public ArrayList<T> entitiesWithParentAndValuesSorted(HandsomeEntity parent, Map<String, Object> values, Map<String, Query.SortDirection> sortDirectionMap){

        Query query = new Query(getKind());

        if ( parent != null)
            query.setAncestor(parent.getKey());

        if (values != null){
            for ( String fieldName : values.keySet()){
                Object fieldValue = values.get(fieldName);
                Query.Filter filter =
                        new Query.FilterPredicate(fieldName,
                                Query.FilterOperator.EQUAL, fieldValue);

                query.setFilter(filter);
            }
        }

        if ( sortDirectionMap != null){

            for ( String value : sortDirectionMap.keySet()){
                query.addSort(value, sortDirectionMap.get(value));
            }
        }

        return queryEntities(query, FetchOptions.Builder.withDefaults());
    }

    /**
     * Queries the com.nomad.handsome.datastore for an {@code Iterable} collection of entities.
     *
     *
     * @param query com.nomad.handsome.datastore query object.
     * @param options query options.
     *
     * @return an {@code Iterable} collection of com.nomad.handsome.datastore entities.
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

    /**
     * Down casts the entity to a NoSQL base entity. The method makes sure the entity is created by
     * NoSQL com.nomad.handsome.datastore module.
     *
     * @param demoEntity the model entity.
     *
     * @return the downcast NoSQL base entity.
     */
    private HandsomeEntity downCastEntity(T demoEntity) {
        Utils.assertTrue(
                demoEntity instanceof HandsomeEntity, "entity has to be a valid NoSQL entity");
        HandsomeEntity entityNoSql = (HandsomeEntity) demoEntity;
        return entityNoSql;
    }

    /**
     * Callback before entity is deleted. Checks if the entity exists.
     *
     * @param ds the com.nomad.handsome.datastore service object.
     * @param demoEntity the entity to be deleted.
     *
     * @return true if the entity should be deleted; otherwise, false.
     */
    protected boolean checkEntityForDelete(DatastoreService ds, HandsomeEntity demoEntity) {
        if (demoEntity != null) {
            Entity entity = demoEntity.getEntity();
            if (entity != null) {
                return getDatastoreEntity(ds, entity.getKey()) != null;
            }
        }
        return false;
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

    /**
     * Creates a model the entity based on com.nomad.handsome.datastore entity.
     *
     * @param entity com.nomad.handsome.datastore entity.
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

        return instance;
    }

    public T newEntitiy() {
        T instance = null;
        try {
            instance = this.entityClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        instance.entity = new Entity( KeyFactory.createKey(getKind(), instance.objectKey));

        return instance;
    }
}
