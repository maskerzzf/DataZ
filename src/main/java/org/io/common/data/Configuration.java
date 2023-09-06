package org.io.common.data;

import lombok.Data;

import java.util.HashMap;

@Data
public class Configuration {
    private HashMap<String,String> sourceDbInfo;
    private HashMap<String,String> targetDbInfo;

}
