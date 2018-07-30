/*******************************************************************************

    Helper struct that wraps a key and value with functions to generate records

    Records can be generated either sequentially or non-sequentially. Both
    functions are deterministic.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.util.Record;

public struct Record
{
    import ocean.io.digest.Fnv1;
    import ocean.text.convert.Formatter;
    import ocean.transition;

    /***************************************************************************

        Record key

    ***************************************************************************/

    hash_t key;

    /***************************************************************************

        Record value

    ***************************************************************************/

    mstring val;

    /// Compares two records, they are same if the key and value is the same
    equals_t opEquals (Record rhs)
    {
        return this.key == rhs.key && this.val == rhs.val;
    }

    /***************************************************************************

        Generates a record from the given index, i. The key of the produced
        record will be equal to i. Thus, if this function is called multiple
        times with incrementing i, the keys of generated records will form a
        sequential series.

        Params:
            i = index of record
            value_index = number of the record for the same key, to generate
                          multiple different values for the same key

        Returns:
            record generated from i

    ***************************************************************************/

    static public Record sequential ( uint i, uint value_index = 0 )
    {
        return Record.fromHash(cast(hash_t)i, value_index);
    }

    /***************************************************************************

        Generates a record from the given index, i. The key of the produced
        record will be equal to the hash of i. Thus, if this function is called
        multiple times with incrementing i, the keys of generated records will
        be essentially randomly ordered (i.e. spread or non-sequential).
        Because of the method used to generate the key (a hash function),
        however, the function is deterministic.

        Params:
            i = index of record
            value_index = number of the record for the same key, to generate
                          multiple different values for the same key

        Returns:
            record generated from i

    ***************************************************************************/

    static public Record spread ( uint i, uint value_index = 0 )
    {
        // Neo's keys are time_t type - they are signed values. This will halve
        // the range of possible input to tests, but they will all fit into
        // time_t without overflowing.
        return Record.fromHash(Fnv1a(i) & ~0x8000_0000_0000_0000, value_index);
    }

    /***************************************************************************

        Generates a record from the given key. The value is set to the string
        representation of the key.

        Params:
            key = key of record
            value_index = number of the record for the same key, to generate
                          multiple different values for the same key

        Returns:
            generated record

    ***************************************************************************/

    static private Record fromHash ( hash_t key, uint value_index = 0 )
    {
        Record r;
        r.key = key;

        if (value_index == 0)
        {
            sformat(r.val, "{}", r.key);
        }
        else
        {
            sformat(r.val, "{}{}", r.key, value_index);
        }

        return r;
    }
}
