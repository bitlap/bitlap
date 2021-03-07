package org.bitlap.common.test.bitmap;

import org.bitlap.common.bitmap.BBM;
import org.bitlap.common.bitmap.CBM;
import org.bitlap.common.bitmap.RBM;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public class CBitMapTest {

    public CBM randCBitMap(short maxBucket, int maxUID, int totalUID, int maxCount) {
        CBM answer = new CBM();
        Random r = new Random();
        int currentID = 0;
        while (currentID < totalUID) {
            short bucket = (short) r.nextInt(maxBucket);
            int id = r.nextInt(maxUID);
            int count = r.nextInt(maxCount) + 1;
            answer.add(bucket, id, count);
            currentID += 1;
        }
        return answer;
    }

    public CBM randCBitMap() {
        return randCBitMap((short) 8, 10000, 10000, 100);
    }


    @Test
    public void testSerialize2() throws IOException, ClassNotFoundException {
        CBM cBitMap = randCBitMap();

        // serialize v2
        byte[] v2 = cBitMap.getBytes();
        CBM cBitMapV2 = new CBM(v2);

        Assert.assertEquals((long)cBitMap.getCount(), (long)cBitMapV2.getCount());
    }

    @Test
    public void testGetCount() {
        CBM cBitMap = new CBM();
        cBitMap.add((short) 0, 1, 10);
        cBitMap.add((short) 3, 4, 20);
        cBitMap.add((short) 1, 2, 30);

        Assert.assertEquals(60L, (long)cBitMap.getCount());
    }

    @Test
    public void testUnionBucketBm() {
        CBM cBitMap = new CBM();
        cBitMap.add((short) 0, 1, 10);
        cBitMap.add((short) 3, 4, 20);
        cBitMap.add((short) 1, 2, 30);
        cBitMap.add((short) 1, 2, 60);

        Assert.assertEquals(3, cBitMap.getCountUnique());
    }

    @Test
    public void testAnd() {
        CBM cBitMap = new CBM();
        cBitMap.add((short) 0, 1, 10);
        cBitMap.add((short) 3, 4, 20);
        cBitMap.add((short) 1, 2, 30);

        BBM bucketBitMap = new BBM();
        bucketBitMap.add((short) 3, 4);
        bucketBitMap.add((short) 1, 2);

        cBitMap.and(bucketBitMap);
        Assert.assertEquals(50, (long)cBitMap.getCount());
    }

    @Test
    public void testAndNot() {
        CBM cBitMap = new CBM();
        cBitMap.add((short) 0, 1, 10);
        cBitMap.add((short) 3, 4, 20);
        cBitMap.add((short) 1, 2, 30);

        BBM bucketBitMap = new BBM();
        bucketBitMap.add((short) 3, 4);
        bucketBitMap.add((short) 1, 3);

        cBitMap.andNot(bucketBitMap);
        Assert.assertEquals(40, (long)cBitMap.getCount());
    }

    @Test
    public void testOrCase1() {
        Random random = new Random(System.currentTimeMillis());
        List<CBM> cbms = new ArrayList<>();
        CBM expectedTotalCbm = new CBM();
        for (int i = 0; i < 1000; i++) {
            CBM cbm = new CBM();
            for (int j = 0; j < 10 + random.nextInt(10); j++) {
                short bucket = (short) random.nextInt(8);
                int uid = Math.abs(random.nextInt());
                long count = Math.abs(random.nextLong());
                cbm.add(bucket, uid, count);
                expectedTotalCbm.add(bucket, uid, count);
            }
            cbms.add(cbm);
        }

        CBM acutalTotalCbm = cbms.stream().reduce(new CBM(), (a, b) -> a.or(b));
        Assert.assertEquals(expectedTotalCbm.getCount(), acutalTotalCbm.getCount(), 0.01);
        Assert.assertEquals(expectedTotalCbm, acutalTotalCbm);
    }

    @Test
    public void testOrCase2() {
        Random random = new Random(System.currentTimeMillis());
        List<CBM> cbms = new ArrayList<>();
        CBM expectedTotalCbm = new CBM();
        for (int i = 0; i < 1000; i++) {
            CBM cbm = new CBM();
            for (int j = 0; j < 10 + random.nextInt(10); j++) {
                final short bucket = (short) -1;
                final int uid = Math.abs(random.nextInt());
                final long count = Math.abs(random.nextLong());
                cbm.add(bucket, uid, count);
                expectedTotalCbm.add(bucket, uid, count);
            }
            cbms.add(cbm);
        }

        CBM acutalTotalCbm = cbms.stream().reduce(new CBM(), (a, b) -> a.or(b));
        Assert.assertEquals((long)expectedTotalCbm.getCount(), (long)acutalTotalCbm.getCount());
        Assert.assertEquals((long)expectedTotalCbm.getCount(), (long)acutalTotalCbm.getCount());
//        Assert.assertEquals(expectedTotalCbm.getUniqueIds(), acutalTotalCbm.getUniqueIds());
//        Assert.assertEquals(expectedTotalCbm.getTotalIds(), acutalTotalCbm.getTotalIds());
    }

    @Test
    public void testOrCase3() {
        final int n = 100000;
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < n; i++) {
            final short bucket = (short) -1;
            final int uid = Math.abs(random.nextInt());
            final long count = random.nextInt(1000);

            CBM expectdCbm = new CBM();

            CBM cbm1 = new CBM();
            cbm1.add(bucket, uid, count);
            CBM cbm2 = new CBM();
            cbm2.add(bucket, uid, count);

            cbm1.or(cbm2);
            expectdCbm.add(bucket, uid, count);
            expectdCbm.add(bucket, uid, count);
            Assert.assertEquals((long)expectdCbm.getCount(), (long)cbm1.getCount());
            cbm1.or(cbm2);
            expectdCbm.add(bucket, uid, count);
            Assert.assertEquals((long)expectdCbm.getCount(), (long)cbm1.getCount());
        }
    }

    @Test
    public void testCompare() {
        CBM cBitMap = new CBM();
        cBitMap.add((short) 0, 1, 10);
        cBitMap.add((short) 3, 4, 20);
        cBitMap.add((short) 1, 2, 30);
        cBitMap.add((short) 0, 2, 40);
        cBitMap.add((short) 4, 3, 50);
        cBitMap.add((short) 2, 5, 60);

        Assert.assertEquals(180, (long)CBM.gte(cBitMap, 50).getCount());
        Assert.assertEquals(180, (long)CBM.gt(cBitMap, 20).getCount());
        Assert.assertEquals(30, (long)CBM.lte(cBitMap, 20).getCount());
        Assert.assertEquals(30, (long)CBM.lt(cBitMap, 50).getCount());

        Assert.assertEquals(6, CBM.lte(cBitMap, 1000).getBBM().getLongCount());
    }

    @Test
    public void testCompareDouble() {
        CBM cBitMap = new CBM();
        cBitMap.add((short) 0, 1, 10);
        cBitMap.add((short) 3, 4, 20);
        cBitMap.add((short) 1, 2, 30);
        cBitMap.add((short) 0, 2, 40);
        cBitMap.add((short) 4, 3, 50);
        cBitMap.add((short) 2, 5, 60);
        cBitMap.setWeight(0.1);

        Assert.assertEquals(18, (long)CBM.gte(cBitMap, 5.0).getCount());
        Assert.assertEquals(18, (long)CBM.gt(cBitMap, 2.0).getCount());
        Assert.assertEquals(3, (long)CBM.lte(cBitMap, 2.0).getCount());
        Assert.assertEquals(3, (long)CBM.lt(cBitMap, 5.0).getCount());
        Assert.assertEquals(6, CBM.lte(cBitMap, 100.0).getBBM().getLongCount());
    }

    @Test
    public void testCompareDouble2() {
        CBM cBitMap = new CBM();
        cBitMap.add((short) 0, 1, 1);
        cBitMap.setWeight(88.0);

        Assert. assertEquals(CBM.equals(cBitMap, 60.0).getCountUnique(), 0);
        Assert.assertEquals(CBM.equals(cBitMap, 88.0).getCountUnique(), 1);
        Assert.assertEquals(CBM.equals(cBitMap, 89.0).getCountUnique(), 0);
    }

    @Test
    public void testCompare2() {
        CBM cBitMap = new CBM();
        cBitMap.add((short)0, 1, 1);
        cBitMap.add((short)0, 1, 1);
        cBitMap.add((short)0, 1, 1);
        cBitMap.add((short)0, 1, 1);
        cBitMap.add((short)0, 1, 1);

        Assert.assertEquals(0, CBM.equals(cBitMap, 1).getCountUnique());
        Assert.assertEquals(0, CBM.equals(cBitMap, 3).getCountUnique());
        Assert.assertEquals(1, CBM.equals(cBitMap, 5).getCountUnique());
        Assert.assertEquals(0, CBM.equals(cBitMap, 7).getCountUnique());
    }

    @Test
    public void testGetSizeInBytes() throws IOException {
        CBM cBitMap = new CBM();
        cBitMap.add((short) 0, 1, 10);
        cBitMap.add((short) 3, 4, 20);
        cBitMap.add((short) 1, 2, 30);
        cBitMap.add((short) 0, 2, 40);
        cBitMap.add((short) 4, 3, 50);
        cBitMap.add((short) 2, 5, 60);

        System.out.println(cBitMap.getSizeInBytes());
        System.out.println(cBitMap.getBytes().length);
    }

    @Test
    public void testFastOr() {
        List<CBM> cbms = new ArrayList<>();
        int nBMs = 10;
        for (int i = 0; i < nBMs; i++) {
            cbms.add(randCBitMap());
        }

        // fast or
        CBM answer1 = CBM.or(cbms.toArray(new CBM[nBMs]));

        // reduce or
        CBM answer2 = new CBM();
        for (CBM bm : cbms) {
            answer2.or(bm);
        }

        Assert.assertTrue((long)answer1.getCount() == (long)answer2.getCount());
    }

    @Test
    public void testOrEmpty() {
        CBM cbm0 = new CBM();
        CBM cbm1 = new CBM();
        cbm1.add((short) -1, 100, 1);
        CBM cbm2 = new CBM();
        cbm2.add((short) -1, 101, 2);
        cbm0.or(cbm1);
        Assert.assertEquals(1, (long)cbm0.getCount());
        cbm0.or(cbm2);
        Assert.assertEquals(3, (long)cbm0.getCount());
    }

    @Test
    public void testTopCount() {
        CBM cbm = new CBM();
        cbm.add((short) 0, 1, 2);
        cbm.add((short) 1, 1, 3);
        cbm.add((short) 1, 2, 3);
        cbm.add((short) 3, 2, 3);
        cbm.add((short) 2, 3, 1);

        Map<Integer, Double> top1 = cbm.getTopCount(1);
        Assert.assertEquals(top1.size(), 1);
        Assert.assertEquals(top1.get(2).longValue(), 6L);

        Map<Integer, Double> top2 = cbm.getTopCount(2);
        Assert.assertEquals(top2.size(), 2);
        Assert.assertEquals(top2.get(2).longValue(), 6L);
        Assert.assertEquals(top2.get(1).longValue(), 5L);

        Map<Integer, Double> top3 = cbm.getTopCount(3);
        Assert.assertEquals(top3.size(), 3);
        Assert.assertEquals(top3.get(2).longValue(), 6L);
        Assert.assertEquals(top3.get(1).longValue(), 5L);
        Assert.assertEquals(top3.get(3).longValue(), 1L);

        Map<Integer, Double> top4 = cbm.getTopCount(4);
        Assert.assertEquals(top4.size(), 3);
        Assert.assertEquals(top4.get(2).longValue(), 6L);
        Assert.assertEquals(top4.get(1).longValue(), 5L);
        Assert.assertEquals(top4.get(3).longValue(), 1L);
    }

    @Test
    public void testEquals() throws IOException {
        CBM cbm0 = new CBM();
        CBM cbm1 = new CBM();
        Random random = new Random(System.currentTimeMillis());

        for (int i = 0; i < 100000; i++) {
            short bucket = (short) random.nextInt(8);
            int uid = random.nextInt(Integer.MAX_VALUE);
            long count = Math.abs(random.nextLong());
            cbm0.add(bucket, uid, count);
            cbm1.add(bucket, uid, count);
        }

        Assert.assertEquals(cbm0, cbm1);
        Assert.assertEquals(cbm0.getBytes().length, cbm1.getBytes().length);

        byte[] b0 = cbm0.getBytes();
        byte[] b1 = cbm1.getBytes();
        Assert.assertArrayEquals(b0, b1);
    }

    @Test
    public void testRunOptimize() {
        CBM cbm = new CBM();
        cbm.add((short) 0, 1, 2L);

        Assert.assertTrue(cbm.getContainer().get(0).isEmpty());
        cbm.repair();
        Assert.assertFalse(cbm.getContainer().containsKey(0));
    }

    @Test
    public void testGetCountDistribution() {
        for (int i = 0; i < 100; i++) {
            CBM cBitMap = randCBitMap();
            double totalCount = cBitMap.getCount();
            Map<Double, RBM> distribution = cBitMap.getDistribution();
            long distributionCount = 0;
            for (double c: distribution.keySet()) {
                distributionCount += c * distribution.get(c).getCount();
            }

            Assert.assertEquals((long)totalCount, distributionCount);
        }
    }


    @Test
    public void testGetCountDistributionWithThreshold() {
        for (int i = 0; i < 100; i++) {
            CBM cBitMap = randCBitMap();
            long maxCount = 10;
            Map<Double, RBM> distribution = cBitMap.getDistribution();
            Map<Double, RBM> distributionWithMaxCount = cBitMap.getDistribution(10);

            for(int j = 1; j < maxCount; j++) {
                RBM r1 = distribution.getOrDefault((double) j, new RBM());
                RBM r2 = distributionWithMaxCount.getOrDefault((double)j, new RBM());
                Assert.assertEquals(r1.getCount(), r2.getCount(), 0.01);
                Assert.assertEquals(RBM.and(r1, r2).getCount(), r2.getCount(), 0.01);
            }

            RBM r1 = new RBM();
            distribution.forEach((k, v) -> {
                if (k >= maxCount) r1.or(v);
            });
            RBM r2 = distributionWithMaxCount.getOrDefault((double)maxCount, new RBM());
            Assert.assertEquals(r1.getCount(), r2.getCount(), 0.01);
            Assert.assertEquals(RBM.and(r1, r2).getCount(), r2.getCount(), 0.01);
        }
    }

    @Test
    public void testCBitMapMulti() {
        for (int i = 0; i < 100; i++) {
            CBM cBitMap = randCBitMap();
            long currentCount = (long)cBitMap.getCount();
            CBM cBitMap1 = CBM.multiBy(cBitMap, i);
            long currentCount1 = (long)cBitMap1.getCount();
            Assert.assertEquals(currentCount * i, currentCount1);
        }
    }

    @Test
    public void testCBitMapAndBucket() {
        CBM c1 = new CBM();
        c1.add((short)-1, 1, 2);
        c1.add((short)-1, 2, 1);
        c1.add((short)1, 2, 1);

        CBM c2 = new CBM();
        c2.add((short)1, 2, 1);
        c2.add((short)2, 3, 5);
        c1.add((short)1, 1, 2);
        c1.add((short)-1, 1, 2);

        CBM c3 = CBM.or(c1, c2);

        BBM dim = new BBM();
        dim.add((short)1, 2);
        dim.add((short)2, 3);
        dim.add((short)1, 1);

        Assert.assertEquals((long)c1.getCount() + (long)c2.getCount(), (long)c3.getCount());
    }

    @Test
    public void testMergeAsRoaringBitmap() {
        CBM cbm = new CBM();
        cbm.add((short)0, 1, 1);
        cbm.add((short)1, 2, 1);
        cbm.add((short)1, 3, 1);
        cbm.add((short)1, 4, 2);
        cbm.add((short)1, 5, 3);
        cbm.add((short)1, 4, 4);
        cbm.add((short)1, 2, 4);
        cbm.add((short)1, 3, 4);

        cbm.add((short)2, 2, 1);
        cbm.add((short)2, 3, 1);
        cbm.add((short)2, 4, 1);
        cbm.add((short)2, 5, 2);
        cbm.add((short)32767, 5, 4);
        cbm.add((short)45678, 6, 1);
        Map<Double, String> m = new HashMap<>();
        m.put(1D, "{1,6}");
        m.put(6D, "{2,3}");
        m.put(7D, "{4}");
        m.put(9D, "{5}");
        for (Map.Entry<Double, RBM> entry: cbm.getDistribution().entrySet()) {
            Double k = entry.getKey();
            String v = entry.getValue().toString();
            Assert.assertEquals(m.get(k), v);
        }
    }
}
