package org.io.common.data;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Data
public class TransitData {
    private HashMap<String, List<DefaultRecord>> recordContainer;
    private HashMap<String, List<ColumnsMetaData>> columnsContainer=new HashMap<>();
    private HashMap<String, List<IndexInfoMetaData>> indexInfoContainer=new HashMap<>();
    private HashMap<String, List<PrimaryKeysMetaData>> primaryKeysContainer=new HashMap<>();

    private HashMap<String, List<ConfigurableColumnsData>> configurableColumnsContainer = new HashMap<>();
}
