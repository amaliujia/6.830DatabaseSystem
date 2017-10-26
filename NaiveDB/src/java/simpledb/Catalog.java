package simpledb;


import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
    private static Logger LOG = Logger.getLogger(Catalog.class);

    // mapping between a table name and table metadata
    private TreeMap<Integer, TableMeta> metaTreeMap;
    private HashMap<String, Integer> nameToTableId;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        metaTreeMap = new TreeMap<Integer, TableMeta>();
        nameToTableId = new HashMap<String, Integer>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public synchronized void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        assert (name != null);
        assert (file != null);

        nameToTableId.put(name, file.getId());
        metaTreeMap.put(file.getId(), new TableMeta(name, pkeyField, file));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if (!nameToTableId.containsKey(name)) {
            throw new NoSuchElementException(String.format("%s does not exist in catalog", name));
        }

        return nameToTableId.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!metaTreeMap.containsKey(tableid)) {
            throw new NoSuchElementException(String.format("%d does not exist in catalog", tableid));
        }

        return metaTreeMap.get(tableid).getFile().getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!metaTreeMap.containsKey(tableid)) {
            throw new NoSuchElementException(String.format("%d does not exist in catalog", tableid));
        }
        return metaTreeMap.get(tableid).getFile();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if (!metaTreeMap.containsKey(tableid)) {
            throw new NoSuchElementException(String.format("%d does not exist in catalog", tableid));
        }
        return metaTreeMap.get(tableid).getPkeyField();
    }

    public Iterator<Integer> tableIdIterator() {
        return metaTreeMap.keySet().iterator();
    }

    /**
     * Returns table name
     * @param id
     *          The id of the table, as specified by the DbFile.getId()
     * @return
     *          String format table name of id exists in catalog,
     *          or null of id does not exist in catalog.
     */
    public String getTableName(int id) {
        // some code goes here
        if (metaTreeMap.containsKey(id)) {
            return metaTreeMap.get(id).getName();
        }
        return null;
    }

    /** Delete all tables from the catalog */
    public synchronized void clear() {
        // some code goes here
        metaTreeMap.clear();
        nameToTableId.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("TABLE NAME: " + name);
                }
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        LOG.error("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            LOG.error("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                LOG.info("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            LOG.error("IO Exception", e);
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            LOG.error("IndexOutOfBoundException", e);
            System.exit(0);
        }
    }
}

