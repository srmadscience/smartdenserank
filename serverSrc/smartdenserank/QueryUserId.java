package smartdenserank;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class QueryUserId extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt getUser = new SQLStmt("SELECT * FROM ratings WHERE comp_id = ? AND user_id = ?;");

    public static final SQLStmt getComp = new SQLStmt("SELECT * FROM competitions WHERE comp_id = ?;");
 
    public static final SQLStmt queryUserIdWithLeagueAndStartOneRank = new SQLStmt(
            "SELECT r.user_id, r.u_rank + c.rank_offset u_rank, r.u_rank -  cb.min_rank + 1 rank_in_league, r.score, cb.display_name  "
                    + "FROM competitions c "
                    + "   , competition_bands cb "
                    + "   , (SELECT * FROM ratings WHERE comp_id = ?  AND u_rank = ? AND user_id >= ? "
                    + " ORDER BY comp_id, u_rank, user_id LIMIT ?) r "
                    + "WHERE r.comp_id = ?  "
                    + "AND   c.comp_id = cb.comp_id  "
                    + "AND   cb.comp_id = r.comp_id  "
                    + "AND   r.u_rank + c.rank_offset BETWEEN cb.min_rank AND cb.max_rank  "
                      + "ORDER BY r.comp_id, r.u_rank, r.user_id;");

    public static final SQLStmt queryUserIdWithLeagueAndStartOneRankReverseSort = new SQLStmt(
            "SELECT * FROM (SELECT r.user_id, r.u_rank + c.rank_offset u_rank, r.u_rank -  cb.min_rank + 1 rank_in_league, r.score, cb.display_name  "
                    + "FROM competitions c "
                    + "   , competition_bands cb "
                    + "   , (SELECT * FROM ratings WHERE comp_id = ?  AND u_rank = ? AND user_id < ? " 
                    + " AND user_id >= ? "
                    + " ORDER BY comp_id, u_rank, user_id DESC LIMIT ?) r "
                    + "WHERE r.comp_id = ?  "
                    + "AND   c.comp_id = cb.comp_id  "
                    + "AND   cb.comp_id = r.comp_id  "
                    + "AND   r.u_rank + c.rank_offset BETWEEN cb.min_rank AND cb.max_rank  "
                    + "ORDER BY r.comp_id, r.u_rank, r.user_id ) as QRY ORDER BY user_id ;");
    
   // @formatter:on

    /**
     * Used for debugging / testing.
     */
    boolean chatty = false;

    
    /**
     * Query by competition and userId, and return up to pageSize 
     * results either side.
     * @param compId
     * @param userId
     * @param pageSize
     * @return A single VoltTable of up to 2x pageSize with userId in the middle
     * @throws VoltAbortException
     */
    public VoltTable[] run(long compId, int userId, int pageSize)
            throws VoltAbortException {

        
        VoltTable[] results = null;

        // Query competition to get offset...
        voltQueueSQL(getComp, EXPECT_ONE_ROW, compId);
        voltQueueSQL(getUser, EXPECT_ONE_ROW, compId, userId);
        
        VoltTable[] vtComp = voltExecuteSQL();
        vtComp[0].advanceRow();
        vtComp[1].advanceRow();

        long evilOffset = vtComp[0].getLong("RANK_OFFSET");
        long userRank = vtComp[1].getLong("U_RANK");
        
        if (chatty) {
            System.out.println("rank offset " + userRank + " " + evilOffset + " " + userId);  
        }
        
       
        int earliestUserIdInt = userId - (pageSize * 100);
 
        voltQueueSQL(queryUserIdWithLeagueAndStartOneRankReverseSort, compId,userRank, userId  , earliestUserIdInt, pageSize, compId);
        voltQueueSQL(queryUserIdWithLeagueAndStartOneRank, compId,userRank, userId  , pageSize+1, compId);
        
        results = voltExecuteSQL(true);

        if (chatty) {
            System.out.println("results " + results[0].getRowCount() + " " + results[1].getRowCount() );  
            System.out.println("results " + results[0].toFormattedString() + " " + results[1].toFormattedString() );  
        }

        
        // Merge our two tables to one...
        results[0] = getWritableCopy(results[0]);
        results[1] = getWritableCopy(results[1]);
        results = mergeVoltTables(results);

        if (chatty) {
            System.out.println("results " + results[0].getRowCount() );  
            System.out.println("results " + results[0].toFormattedString() );  
        }

        
        if (results[0].getRowCount() < pageSize) {
            //TODO
            // If we get here it's because because we've exhausted the entries in this rank.
            // We can examine ROWS_PER_SCORE_VIEW to see how many rows there 
            // are but where we go from there is TODO right now.
        }
         
        return results;

    }

    /**
     * Collapse an array of VoltTable into a single one. Assumes 
     * that all the tables have the same structure.
     * @param voltTables Array of VoltTable.
     * @return A single entry array of VoltTable
     */
    private static VoltTable[] mergeVoltTables(VoltTable[] vt) {
        
        VoltTable[] newTabs = new VoltTable[1];
        newTabs[0] = new VoltTable(vt[0].getTableSchema());
        
        for (int i=0; i < vt.length; i++) {
            vt[i].resetRowPosition();
            while (vt[i].advanceRow()) {
                newTabs[0].add(vt[i].cloneRow());
            }
           
        }
        
       return newTabs;
    }

    /**
     * By default VoltTable objects are 'final'. This method allows us to create
     * a writable one.
     * 
     * @param oldVoltTable
     *            VoltTable we want a writable copy of.
     * @return A writable copy of oldVoltTable
     */
    private static VoltTable getWritableCopy(VoltTable oldVoltTable) {

        if (oldVoltTable == null) {
            return null;
        }

        VoltTable copy = new VoltTable(oldVoltTable.getTableSchema());

        while (oldVoltTable.advanceRow()) {
            copy.add(oldVoltTable.cloneRow());
        }

        return copy;
    }
}
