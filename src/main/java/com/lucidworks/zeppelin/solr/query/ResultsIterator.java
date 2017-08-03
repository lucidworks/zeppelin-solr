package com.lucidworks.zeppelin.solr.query;

import java.util.Iterator;

public abstract class ResultsIterator<E> implements Iterator<E>, Iterable<E> {

  public abstract long getNumDocs();

}
