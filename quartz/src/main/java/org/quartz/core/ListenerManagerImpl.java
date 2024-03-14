package org.quartz.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.ListenerManager;
import org.quartz.Matcher;
import org.quartz.SchedulerListener;
import org.quartz.TriggerKey;
import org.quartz.TriggerListener;
import org.quartz.impl.matchers.EverythingMatcher;

public class ListenerManagerImpl implements ListenerManager {

    private Map<String, JobListener> globalJobListeners = new LinkedHashMap<String, JobListener>(10);

    private final ReadWriteLock globalJobListenersLock = new ReentrantReadWriteLock(true);

    private Map<String, TriggerListener> globalTriggerListeners = new LinkedHashMap<String, TriggerListener>(10);

    private final ReadWriteLock globalTriggerListenersLock = new ReentrantReadWriteLock(true);

    private Map<String, List<Matcher<JobKey>>> globalJobListenersMatchers = new LinkedHashMap<String, List<Matcher<JobKey>>>(10);

    private Map<String, List<Matcher<TriggerKey>>> globalTriggerListenersMatchers = new LinkedHashMap<String, List<Matcher<TriggerKey>>>(10);

    private ArrayList<SchedulerListener> schedulerListeners = new ArrayList<SchedulerListener>(10);

    
    public void addJobListener(JobListener jobListener, Matcher<JobKey> ... matchers) {
        addJobListener(jobListener, Arrays.asList(matchers));
    }

    public void addJobListener(JobListener jobListener, List<Matcher<JobKey>> matchers) {
        if (jobListener.getName() == null || jobListener.getName().length() == 0) {
            throw new IllegalArgumentException(
                    "JobListener name cannot be empty.");
        }

        globalJobListenersLock.writeLock().lock();
        try {
            globalJobListeners.put(jobListener.getName(), jobListener);
            LinkedList<Matcher<JobKey>> matchersL = new  LinkedList<Matcher<JobKey>>();
            if(matchers != null && matchers.size() > 0)
                matchersL.addAll(matchers);
            else
                matchersL.add(EverythingMatcher.allJobs());
            
            globalJobListenersMatchers.put(jobListener.getName(), matchersL);
        }
        finally {
            globalJobListenersLock.writeLock().unlock();
        }
    }


    public void addJobListener(JobListener jobListener) {
        addJobListener(jobListener, EverythingMatcher.allJobs());
    }
    
    public void addJobListener(JobListener jobListener, Matcher<JobKey> matcher) {
        if (jobListener.getName() == null || jobListener.getName().length() == 0) {
            throw new IllegalArgumentException(
                    "JobListener name cannot be empty.");
        }

        globalJobListenersLock.writeLock().lock();
        try {
            globalJobListeners.put(jobListener.getName(), jobListener);
            LinkedList<Matcher<JobKey>> matchersL = new  LinkedList<Matcher<JobKey>>();
            if(matcher != null)
                matchersL.add(matcher);
            else
                matchersL.add(EverythingMatcher.allJobs());
            
            globalJobListenersMatchers.put(jobListener.getName(), matchersL);
        }
        finally {
            globalJobListenersLock.writeLock().unlock();
        }
    }


    public boolean addJobListenerMatcher(String listenerName, Matcher<JobKey> matcher) {
        if(matcher == null)
            throw new IllegalArgumentException("Null value not acceptable.");

        globalJobListenersLock.writeLock().lock();
        try {
            List<Matcher<JobKey>> matchers = globalJobListenersMatchers.get(listenerName);
            if(matchers == null)
                return false;
            matchers.add(matcher);
            return true;
        }
        finally {
            globalJobListenersLock.writeLock().unlock();
        }
    }

    public boolean removeJobListenerMatcher(String listenerName, Matcher<JobKey> matcher) {
        if(matcher == null)
            throw new IllegalArgumentException("Non-null value not acceptable.");

        globalJobListenersLock.writeLock().lock();
        try {
            List<Matcher<JobKey>> matchers = globalJobListenersMatchers.get(listenerName);
            if(matchers == null)
                return false;
            return matchers.remove(matcher);
        }
        finally {
            globalJobListenersLock.writeLock().unlock();
        }
    }

    public List<Matcher<JobKey>> getJobListenerMatchers(String listenerName) {
        globalJobListenersLock.readLock().lock();
        try {
            List<Matcher<JobKey>> matchers = globalJobListenersMatchers.get(listenerName);
            if(matchers == null)
                return null;
            return Collections.unmodifiableList(matchers);
        }
        finally {
            globalJobListenersLock.readLock().unlock();
        }
    }

    public boolean setJobListenerMatchers(String listenerName, List<Matcher<JobKey>> matchers)  {
        if(matchers == null)
            throw new IllegalArgumentException("Non-null value not acceptable.");

        globalJobListenersLock.writeLock().lock();
        try {
            List<Matcher<JobKey>> oldMatchers = globalJobListenersMatchers.get(listenerName);
            if(oldMatchers == null)
                return false;
            globalJobListenersMatchers.put(listenerName, matchers);
            return true;
        }
        finally {
            globalJobListenersLock.writeLock().unlock();
        }
    }


    public boolean removeJobListener(String name) {
        globalJobListenersLock.writeLock().lock();
        try {
            return (globalJobListeners.remove(name) != null);
        }
        finally {
            globalJobListenersLock.writeLock().unlock();
        }
    }
    
    public List<JobListener> getJobListeners() {
        globalJobListenersLock.readLock().lock();
        try {
            return java.util.Collections.unmodifiableList(new LinkedList<JobListener>(globalJobListeners.values()));
        }
        finally {
            globalJobListenersLock.readLock().unlock();
        }
    }

    public JobListener getJobListener(String name) {
        globalJobListenersLock.readLock().lock();
        try {
            return globalJobListeners.get(name);
        }
        finally {
            globalJobListenersLock.readLock().unlock();
        }
    }

    public void addTriggerListener(TriggerListener triggerListener, Matcher<TriggerKey> ... matchers) {
        addTriggerListener(triggerListener, Arrays.asList(matchers));
    }
    
    public void addTriggerListener(TriggerListener triggerListener, List<Matcher<TriggerKey>> matchers) {
        if (triggerListener.getName() == null
                || triggerListener.getName().length() == 0) {
            throw new IllegalArgumentException(
                    "TriggerListener name cannot be empty.");
        }

        globalTriggerListenersLock.writeLock().lock();
        try {
            globalTriggerListeners.put(triggerListener.getName(), triggerListener);

            LinkedList<Matcher<TriggerKey>> matchersL = new  LinkedList<Matcher<TriggerKey>>();
            if(matchers != null && matchers.size() > 0)
                matchersL.addAll(matchers);
            else
                matchersL.add(EverythingMatcher.allTriggers());

            globalTriggerListenersMatchers.put(triggerListener.getName(), matchersL);
        }
        finally {
            globalTriggerListenersLock.writeLock().unlock();
        }
    }
    
    public void addTriggerListener(TriggerListener triggerListener) {
        addTriggerListener(triggerListener, EverythingMatcher.allTriggers());
    }

    public void addTriggerListener(TriggerListener triggerListener, Matcher<TriggerKey> matcher) {
        if(matcher == null)
            throw new IllegalArgumentException("Null value not acceptable for matcher.");
        
        if (triggerListener.getName() == null
                || triggerListener.getName().length() == 0) {
            throw new IllegalArgumentException(
                    "TriggerListener name cannot be empty.");
        }

        globalTriggerListenersLock.writeLock().lock();
        try {
            globalTriggerListeners.put(triggerListener.getName(), triggerListener);
            List<Matcher<TriggerKey>> matchers = new LinkedList<Matcher<TriggerKey>>();
            matchers.add(matcher);
            globalTriggerListenersMatchers.put(triggerListener.getName(), matchers);
        }
        finally {
            globalTriggerListenersLock.writeLock().unlock();
        }
    }

    public boolean addTriggerListenerMatcher(String listenerName, Matcher<TriggerKey> matcher) {
        if(matcher == null)
            throw new IllegalArgumentException("Non-null value not acceptable.");

        globalTriggerListenersLock.writeLock().lock();
        try {
            List<Matcher<TriggerKey>> matchers = globalTriggerListenersMatchers.get(listenerName);
            if(matchers == null)
                return false;
            matchers.add(matcher);
            return true;
        }
        finally {
            globalTriggerListenersLock.writeLock().unlock();
        }
    }

    public boolean removeTriggerListenerMatcher(String listenerName, Matcher<TriggerKey> matcher) {
        if(matcher == null)
            throw new IllegalArgumentException("Non-null value not acceptable.");

        globalTriggerListenersLock.writeLock().lock();
        try {
            List<Matcher<TriggerKey>> matchers = globalTriggerListenersMatchers.get(listenerName);
            if(matchers == null)
                return false;
            return matchers.remove(matcher);
        }
        finally {
            globalTriggerListenersLock.writeLock().unlock();
        }
    }

    public List<Matcher<TriggerKey>> getTriggerListenerMatchers(String listenerName) {
        globalTriggerListenersLock.readLock().lock();
        try {
            List<Matcher<TriggerKey>> matchers = globalTriggerListenersMatchers.get(listenerName);
            if(matchers == null)
                return null;
            return Collections.unmodifiableList(matchers);
        }
        finally {
            globalTriggerListenersLock.readLock().unlock();
        }
    }

    public boolean setTriggerListenerMatchers(String listenerName, List<Matcher<TriggerKey>> matchers)  {
        if(matchers == null)
            throw new IllegalArgumentException("Non-null value not acceptable.");

        globalTriggerListenersLock.writeLock().lock();
        try {
            List<Matcher<TriggerKey>> oldMatchers = globalTriggerListenersMatchers.get(listenerName);
            if(oldMatchers == null)
                return false;
            globalTriggerListenersMatchers.put(listenerName, matchers);
            return true;
        }
        finally {
            globalTriggerListenersLock.writeLock().unlock();
        }
    }

    public boolean removeTriggerListener(String name) {
        globalTriggerListenersLock.writeLock().lock();
        try {
            return (globalTriggerListeners.remove(name) != null);
        }
        finally {
            globalTriggerListenersLock.writeLock().unlock();
        }
    }
    

    public List<TriggerListener> getTriggerListeners() {
        globalTriggerListenersLock.readLock().lock();
        try {
            return java.util.Collections.unmodifiableList(new LinkedList<TriggerListener>(globalTriggerListeners.values()));
        }
        finally {
            globalTriggerListenersLock.readLock().unlock();
        }
    }

    public TriggerListener getTriggerListener(String name) {
        globalTriggerListenersLock.readLock().lock();
        try {
            return globalTriggerListeners.get(name);
        }
        finally {
            globalTriggerListenersLock.readLock().unlock();
        }
    }
    
    
    public void addSchedulerListener(SchedulerListener schedulerListener) {
        synchronized (schedulerListeners) {
            schedulerListeners.add(schedulerListener);
        }
    }

    public boolean removeSchedulerListener(SchedulerListener schedulerListener) {
        synchronized (schedulerListeners) {
            return schedulerListeners.remove(schedulerListener);
        }
    }

    public List<SchedulerListener> getSchedulerListeners() {
        synchronized (schedulerListeners) {
            return java.util.Collections.unmodifiableList(new ArrayList<SchedulerListener>(schedulerListeners));
        }
    }
}
