package com.ask.nrelate.mr.ron;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.net.URI;

public class LogNormalizerMapperTest {
/*    public static void main(String args[]){
        //LogNormalizerMapper logNormalizerMapper = new LogNormalizerMapper();
        try {
            new LogNormalizerMapperTest().testInternalClickMapper();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        *//*logNormalizerMapper.test("36.68.166.121 - - [17/Jan/2013:02:14:56 -0700] \"GET /tracking/?plugin=rc&type=internal&domain=www.malaysiantabloid.com&src_url=http%3A%2F%2Fwww.malaysiantabloid.com%2F2012%2F03%2F3-artis-wanita-cun-ini-yang-sudah-murtad_3446.html&dest_url=http%3A%2F%2Fwww.malaysiantabloid.com%2F2012%2F03%2Fawek-cantik-ini-nekad-bunuh-diri_1273.html HTTP/1.1\" 200 255 \"http://www.malaysiantabloid.com/2012/03/3-artis-wanita-cun-ini-yang-sudah-murtad_3446.html\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.2.15 Version/10.00\"");
        //logNormalizerMapper.test("115.186.64.230 - - [17/Jan/2013:02:14:56 -0700] \"GET /rcw_wp/0.45.1/?tag=nrelate_related&keywords=Watch+Living+on+the+Edge+Episode+12&domain=www.apniisp.com%2Fblog&url=http%3A%2F%2Fwww.apniisp.com%2Fblog%2Fwatch-living-on-the-edge-episode-12-2 HTTP/1.1\" 200 1099 \"http://www.apniisp.com/blog/watch-living-on-the-edge-episode-12-2\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3\"");
        //logNormalizerMapper.test("207.200.116.66 - - [17/Jan/2013:00:30:34 -0700] \"GET /r2/?type=ad&plugin=rc&cid=21&domain=psychcentral.com%2Fnews&src_url=http%3A%2F%2Fpsychcentral.com%2Fnews%2F2012%2F08%2F14%2Fbipolar-patients-with-history-of-pot-use-show-better-cognitive-skills%2F43096.html&dest_url=http%3A%2F%2Fmental.healthguru.com%2Fvideo%2Fbipolar-treatment-medications%3Fhgref%3Dnrelate%26utm_source%3Dpsychcentral.com%2Fnews HTTP/1.1\" 200 455 \"http://psychcentral.com/news/2012/08/14/bipolar-patients-with-history-of-pot-use-show-better-cognitive-skills/43096.html\" \"Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.0.3705; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)\"");*//**//*
    }*/

    @Test
    public void testInternalClickMapper() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
                , configuration);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("36.68.166.121 - - [18/Jan/2013:02:14:56 -0700] \"GET /tracking/?plugin=rc&type=internal&domain=www.malaysiantabloid.com&src_url=http%3A%2F%2Fwww.malaysiantabloid.com%2F2012%2F03%2F3-artis-wanita-cun-ini-yang-sudah-murtad_3446.html&dest_url=http%3A%2F%2Fwww.malaysiantabloid.com%2F2012%2F03%2Fawek-cantik-ini-nekad-bunuh-diri_1273.html HTTP/1.1\" 200 255 \"http://www.malaysiantabloid.com/2012/03/3-artis-wanita-cun-ini-yang-sudah-murtad_3446.html\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.2.15 Version/10.00\""))
                .runTest();
    }

    @Test
    public void testAdClickMapper() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        /*DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
                , configuration);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);*/
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("173.59.57.9\t-\t-\t[18/Jun/2013:08:24:06 +0000]\t\"GET /r3/?type=ad&plugin=rc&widget_id=106&page_type_id=2100&article_id=246740&cid=11494&domain=www.techrepublic.com&src_url=http%3A%2F%2Fwww.techrepublic.com%2Fblog%2Fwindow-on-windows%2Fuse-this-excel-quick-fill-handle-trick-to-insert-partial-rows-and-columns%2F7776&dest_url=http%3A%2F%2Fwww.hacksurfer.com%2Famplifications%2F1&nrid=0651e07ebdb8f0fc78d996b848e2ef87&iml=63&tml=63&policy=B0020-19 HTTP/1.1\"\t200\t178\t\"http://www.techrepublic.com/blog/opensource/how-linux-found-its-home-in-the-enterprise/4350\"\t\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36\"\t10.10.10.56\t10.10.10.70"))
                .runTest();
    }

    @Test
    public void testImpressionMapper() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        //DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
            //    , configuration);
        //DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("120.168.1.200\t-\t-\t[02/Sep/2013:10:15:27 +0000]\t\"GET /rcw_b/0.52.0/?tag=nrelate_related&domain=www.tahutek.net&keywords=12%20Ekstensi%20GNOME%20Shell%20Paling%20Fungsional&url=&nr_div_number=0&pr_id=AlL1LFDiTJnMeaR9LvTdJZ8R&referrer=http%3A%2F%2Fwww.tahutek.net%2Fsearch%3Fupdated-max%3D2012-10-09T00%3A13%3A00%252B07%3A00%26max-results%3D10%26start%3D20%26by-date%3Dfalse&loading_time=22054&cs_time=74&parse_time=9 HTTP/1.1\"\t200\t831\t\"http://www.tahutek.net/2012/07/12-ekstensi-gnome-shell-paling.html\"\t\"Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0\"\t10.10.10.51\t10.10.10.199"))
                .runTest();
    }

    @Test
    public void testRealImpressionMapper() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
                , configuration);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("41.135.156.121\t-\t-\t[18/Jan/2013:22:39:37 -0800]\t\"GET /vt/?plugin=rc&domain=www.gamespot.com&url=http%3A%2F%2Fwww.gamespot.com%2Fcall-of-duty-black-ops-ii%2Fvideos%2Fcall-of-duty-black-ops-ii-nuketown-2025-trailer-6399315%2F&widget_id=100&page_type_id=6475&page_type=gamespace&geo=US&pr_id=108ThP2Gof3gEaAJ2HKsJEsy&top=887&left=1115 HTTP/1.1\"\t200\t-\t\"-\"\t\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229.94 Safari/537.4\"\t10.10.10.74\t10.10.10.67"))
                .runTest();
    }

    @Test
    public void testBotLogs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
                , configuration);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
/*        mapDriver.withMapper(new LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("98.206.160.241\t-\t-\t[18/Jan/2013:00:10:00 -0700]\t\"GET /rcw_b/0.50.0/?domain=www.ikeahackers.net&keywords=Hacker%20help%3A%20Expedit%20Working-Desk%20-%20good%20idea%3F&url=http%3A%2F%2Fwww.ikeahackers.net%2F2012%2F08%2Fhacker-help-expedit-working-desk-good.html&nr_div_number=9&source=hp HTTP/1.1\"\t200\t1042\t\"-\"\t\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.83 Safari/537.1 www.google.com/bot.html\""))
                .runTest();*/
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("69.160.84.35\t-\t-\t[18/Jan/2013:00:10:00 -0700]\t\"GET /rcw_wp/0.51.2/?tag=nrelate_related&keywords=DO%E2%80%99s+And+DONTs+Of+Backing+Up+Your+PC&domain=www.besttechtips.net&url=http%3A%2F%2Fwww.besttechtips.net%2Fwindows-tips%2Fdos-and-donts-of-backing-up-your-pc%2F&nr_div_number=1&nonjs=1 HTTP/1.0\"\t200\t6822\t\"-\"\t\"WordPress/3.4.1; http://www.besttechtips.net\""))
                .runTest();
    }

    @Test
    public void testInvalidLogs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
                , configuration);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
        /*mapDriver.withMapper(new LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("98.206.160.241-\t-\t[18/Jan/2013:00:10:00 -0700]\t\"GET /rcw_b/0.50.0/?domain=www.ikeahackers.net&keywords=Hacker%20help%3A%20Expedit%20Working-Desk%20-%20good%20idea%3F&url=http%3A%2F%2Fwww.ikeahackers.net%2F2012%2F08%2Fhacker-help-expedit-working-desk-good.html&nr_div_number=9&source=hp HTTP/1.1\"\t200\t1042\t\"-\"\t\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.83 Safari/537.1 www.google.com/bot.html\""))
                .runTest();*/
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("-\t-\t-\t[29/Aug/2012:00:09:59 -0700]\t\"GET /api/1.2a/nrelatesearch.php?format=php_array&timespan=525600&fq=rssid:6071042&fields=title,link,score,media,datetime&max_title=50&keyCode=756b84ae169567b931528b66dfca4c1a&max_results=12&min_rank_score=0&min_score=0.05&keywords=Rep.+Todd+Akin+Explains+%27Legitimate+Rape%27 HTTP/1.1\"\t200\t6078\t\"-\"\t\"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.1) Gecko/20061204 Firefox/2.0.0.1 nRelate\""))
                .runTest();
    }


    @Test
    public void testRTImpressionMapper() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        //DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
        //    , configuration);
        //DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("{\"src_dom\":\"123gag.fr\",\"src_did\":\"561775\",\"plugin\":\"mp\",\"int\":[{\"inturl\":\"http:\\/\\/123gag.fr\\/google-image-obama-vs-hollande\\/\",\"OPEDID\":\"e4a45bf0fea1b90c21adbb27dea17e5e\"},{\"inturl\":\"http:\\/\\/123gag.fr\\/25-photos-prises-pile-au-bon-moment\\/\",\"OPEDID\":\"486a581c22c5d235b2261354af5dc610\"},{\"inturl\":\"http:\\/\\/123gag.fr\\/bonne-chance-les-gars-une-pense-pour-eux-svp\\/\",\"OPEDID\":\"1f02f6febc159e6045f747add51bb35e\"},{\"inturl\":\"http:\\/\\/123gag.fr\\/les-38-pires-photos-de-mariage\\/\",\"OPEDID\":\"c61dea0c883248fafcbd183b93c43093\"}],\"ad\":[],\"scriptdir\":\"\\/var\\/www\\/mpw_wp\\/0.51.0\",\"version\":\"0.52.6\",\"country\":\"\",\"mobile\":\"\",\"t\":\"v\",\"cl_ip\":\"127.0.0.1\",\"ws_ip\":\"10.10.10.55\",\"ts\":1385079360077}"))
                .runTest();
    }

    @Test
    public void testRTExternalMapper() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        //DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
        //    , configuration);
        //DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("{\"type\":\"external\",\"plugin\":\"rc\",\"src_dom\":\"theluckeystar.com\",\"src_url\":\"http:\\/\\/theluckeystar.com\\/eyecandy-samson\\/\",\"dst_url\":\"http:\\/\\/industrykingzxxx.com\\/thug-seduction-meet-samson\\/\",\"policy\":\"\",\"t\":\"c\",\"widget_id\":\"0\",\"page_type_id\":\"0\",\"pr_id\":\"wMkw5jsVA5clmVzlvX1B2mBn\",\"ua\":\"Mozilla\\/5.0 (Windows NT 6.0) AppleWebKit\\/537.36 (KHTML, like Gecko) Chrome\\/30.0.1599.101 Safari\\/537.36\",\"cl_ip\":\"50.150.220.233\",\"src_did\":567192,\"ws_ip\":\"10.10.10.46\",\"ts\":1384228806922}"))
                .runTest();
    }

    @Test
    public void testRTInternalMapper() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");
        //DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/cache/suspectClick/suspectFrequency.dat")
        //    , configuration);
        //DistributedCache.addCacheFile(new URI("hdfs://localhost:10011/GeoIP.dat"), configuration);
        MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable,Text,Text, Text>();
        mapDriver.setConfiguration(configuration);
        mapDriver.withMapper(new com.ask.nrelate.rt.mr.LogNormalizerMapper())
                .withInput(new LongWritable(4),new Text("{\"type\":\"internal\",\"plugin\":\"rc\",\"src_dom\":\"news.cnet.com\",\"src_url\":\"http:\\/\\/news.cnet.com\\/8301-1023_3-57610145-93\\/isohunt-bittorrent-site-rises-from-the-dead-as-isohunt.to\\/\",\"dst_url\":\"http:\\/\\/news.cnet.com\\/8301-1035_3-57613793-94\\/iphone-5s-or-galaxy-s4-owner-whats-your-problem\\/\",\"OPEDID\":\"d2eb1db27d01c508ad20a66c6fe43218\",\"policy\":\"B0037-0\",\"t\":\"c\",\"widget_id\":\"288\",\"page_type_id\":\"8301\",\"pr_id\":\"T2djIr17e8FdKLPfBQkpfXO4\",\"ws_ip\":\"10.10.10.70\",\"ts\":1385946187737,\"src_did\":5000085}"))
                .runTest();
    }

}
