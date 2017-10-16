package simpledb;


import lombok.Getter;
import lombok.Setter;

public class TableMeta {
    @Setter
    @Getter
    private String name;

    @Setter
    @Getter
    private String pkeyField;

    @Setter
    @Getter
    private DbFile file;

    public TableMeta(String name, String pkeyField, DbFile file) {
        this.setName(name);
        this.setPkeyField(pkeyField);
        this.setFile(file);
    }
}