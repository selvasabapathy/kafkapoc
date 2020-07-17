package com.sabapathy.kafkapoc.database;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class DatabaseModeIdentifier
{
    @Value("${kafka.brconsent.listener.isPrimaryDB}")
    private boolean isPrimaryDB;
    
    public boolean isPrimary()
    {
        return isPrimaryDB;
    }
}
