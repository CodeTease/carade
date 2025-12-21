package core.persistence.rdb;

public class RdbConstants {
    // Magic and Version
    public static final String RDB_MAGIC = "REDIS";
    public static final int RDB_VERSION = 9;

    // OpCodes
    public static final int RDB_OPCODE_EOF = 0xFF;
    public static final int RDB_OPCODE_SELECTDB = 0xFE;
    public static final int RDB_OPCODE_EXPIRETIME = 0xFD;
    public static final int RDB_OPCODE_EXPIRETIMEMS = 0xFC;
    public static final int RDB_OPCODE_RESIZEDB = 0xFB;
    public static final int RDB_OPCODE_AUX = 0xFA;

    // Value Types
    public static final int RDB_TYPE_STRING = 0;
    public static final int RDB_TYPE_LIST = 1;
    public static final int RDB_TYPE_SET = 2;
    public static final int RDB_TYPE_ZSET = 3;
    public static final int RDB_TYPE_HASH = 4;
    public static final int RDB_TYPE_ZSET_2 = 5; // Not used often but exists
    public static final int RDB_TYPE_MODULE = 6;
    public static final int RDB_TYPE_MODULE_2 = 7;
    public static final int RDB_TYPE_HASH_ZIPMAP = 9;
    public static final int RDB_TYPE_LIST_ZIPLIST = 10;
    public static final int RDB_TYPE_SET_INTSET = 11;
    public static final int RDB_TYPE_ZSET_ZIPLIST = 12;
    public static final int RDB_TYPE_HASH_ZIPLIST = 13;
    public static final int RDB_TYPE_LIST_QUICKLIST = 14;

    // Length Encodings
    public static final int RDB_6BITLEN = 0;
    public static final int RDB_14BITLEN = 1;
    public static final int RDB_32BITLEN = 0x80; // This is a bit weird, usually it's checked by mask
    public static final int RDB_ENCVAL = 3;
    
    // Special Encodings (when ENCVAL)
    public static final int RDB_ENC_INT8 = 0;
    public static final int RDB_ENC_INT16 = 1;
    public static final int RDB_ENC_INT32 = 2;
    public static final int RDB_ENC_LZF = 3;
    
    // Limits
    public static final int RDB_LOAD_NONE = 0;
    public static final int RDB_LOAD_ENC = (1<<0);
    public static final int RDB_LOAD_PLAIN = (1<<1);
    public static final int RDB_LOAD_SDS = (1<<2);
}
