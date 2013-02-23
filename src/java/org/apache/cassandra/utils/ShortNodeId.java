package org.apache.cassandra.utils;

import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Tracks the network topology determined by the property file snitch to
 * determine the short node id we can embed in versions (timestamps)
 *
 * ShortNodeIds are 16 bits, high 4 bits identify the datacenter,
 * low 12 bits identify the node within that datacenter.
 *
 * With this setup we can handle up to 8 datacenters with up to 4096 servers in each
 *
 * @author wlloyd
 *
 */
public class ShortNodeId {
    //TODO: Keep this consistent across node failures and additions
    private static Map<InetAddress, Short> addrToId = new HashMap<InetAddress, Short>();

    public static void updateShortNodeIds(Map<InetAddress, String[]> addrToDcAndRack)
    {
	synchronized (addrToId) {
	    //Just doing the brain-deadest thing for now
	    addrToId.clear();

	    SortedMap<String, List<InetAddress>> datacenterToAddrs = new TreeMap<String, List<InetAddress>>();
	    for (Entry<InetAddress, String[]> entry : addrToDcAndRack.entrySet()) {
		InetAddress addr = entry.getKey();
		String datacenter = entry.getValue()[0];

		if (!datacenterToAddrs.containsKey(datacenter)) {
		    datacenterToAddrs.put(datacenter, new ArrayList<InetAddress>());
		}
		datacenterToAddrs.get(datacenter).add(addr);
	    }

	    byte dcIndex = 0;
	    for (List<InetAddress> addrs : datacenterToAddrs.values()) {
		short nodeIndex = 0;
		for (InetAddress addr : addrs) {
		    assert dcIndex < 8 && nodeIndex < 4096;
		    short fullIndex = (short) ((dcIndex << 12) + nodeIndex);
		    addrToId.put(addr, fullIndex);
		    nodeIndex++;
		}
		dcIndex++;
	    }
	    LamportClock.setLocalId(getLocalId());
	}
    }

    public static short getId(InetAddress addr)
    {
	synchronized (addrToId) {
	    assert addrToId.containsKey(addr) : "addr = " + addr + " not found in " + addrToId;
	    return addrToId.get(addr).shortValue();
	}
    }

    public static byte getDC(InetAddress addr)
    {
	synchronized (addrToId) {
	    assert addrToId.containsKey(addr) : "addr = " + addr + " not found in " + addrToId;
	    return (byte) (addrToId.get(addr).shortValue() >> 12);
	}
    }


    public static short getLocalId()
    {
        return getId(DatabaseDescriptor.getListenAddress());
    }

    public static Set<InetAddress> getNonLocalAddressesInThisDC()
    {
        Set<InetAddress> nonLocalAddresses = new HashSet<InetAddress>();
        byte localDC = getLocalDC();
        for (InetAddress addr : addrToId.keySet()) {
            if (getDC(addr) == localDC && addr != DatabaseDescriptor.getListenAddress()) {
                nonLocalAddresses.add(addr);
            }
        }
        return nonLocalAddresses;
    }

    public static byte getLocalDC()
    {
        return getDC(DatabaseDescriptor.getListenAddress());
    }

}
