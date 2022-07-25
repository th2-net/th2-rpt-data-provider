package com.exactpro.th2.rptdataprovider.services.cradle

//TODO Temporary wrapper to fix NPE from Cradle when trying to call hasNext() a second time in a loop if that iteration received an item via next()

class CradleIteratorWrapper<T>(private val iterator: Iterator<T>) : Iterator<T> {
    private var hasNext: Boolean;

    init {
        hasNext = iterator.hasNext()
    }

    override fun hasNext(): Boolean {
        return hasNext
    }

    override fun next(): T {
        return iterator.next().also { hasNext = iterator.hasNext() }
    }
}