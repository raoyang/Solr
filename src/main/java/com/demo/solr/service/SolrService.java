package com.demo.solr.service;

import com.demo.solr.constent.SolrConstent;
import com.demo.solr.domain.SolrAccount;
import com.demo.solr.util.SolrUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;

@Service
public class SolrService {

    private static final Logger logger = LoggerFactory.getLogger(SolrService.class);

    @Autowired
    private SolrClient solrClient;

    /***
     * 添加一个索引记录
     * @param collection
     * @param account
     * @return
     * @throws Exception
     */
    public boolean addSolrAccount(String collection, SolrAccount account) throws Exception {
        try {
            String collect = collection == null ? SolrConstent.DEFAULT_COLLECTION : collection;
            solrClient.addBean(collect, account);
            solrClient.commit(collect);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    /***
     * 更新solr账户索引数据
     * @param collection
     * @param account
     * @return
     */
    public boolean updateSolrAccount(String collection, SolrAccount account) throws Exception{
        try{
            String collect = collection == null ? SolrConstent.DEFAULT_COLLECTION : collection;
            solrClient.addBean(collect, account);
            solrClient.commit(collect);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }


    /***
     * 查找solr仓库
     * @param collection
     * @param input
     * @param index
     * @return
     * @throws Exception
     */
    public List<SolrAccount> searchSolrAccount(String collection, SolrAccount input, int index) throws Exception{
        try{
            String collect = collection == null ? SolrConstent.DEFAULT_COLLECTION : collection;
            SolrQuery params = new SolrQuery();
            params.set("q", SolrUtil.getKeywords(input));
            params.setStart(index * SolrConstent.INDEX_COUNT);
            params.setRows(SolrConstent.INDEX_COUNT);
            QueryResponse queryResponse = solrClient.query(collect, params);
            SolrDocumentList results = queryResponse.getResults();
            if(results.getNumFound() == 0){
                return null; //没有检索到数据
            }
            List<SolrAccount> result = new ArrayList<>();
            for(SolrDocument doc : results){
                result.add(SolrUtil.getSolrDocument(doc));
            }
            return result;
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }


    /***
     * 仅供测试使用接口
     * @param input
     * @return
     */
    public boolean delete(String input){
        try{
            solrClient.deleteByQuery("user","*:*");
            solrClient.commit("user");
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
