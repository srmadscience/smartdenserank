package smartdenserank;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class QueryRank extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt queryUserIdWithLeague = new SQLStmt(       
            "SELECT r.user_id, r.u_rank + c.rank_offset u_rank, r.u_rank -  cb.min_rank + 1 rank_in_league, r.score, cb.display_name  "
                    + "FROM competitions c "
                    + "   , competition_bands cb "
                    + "   , ratings r  "
                    + "WHERE r.comp_id = ?  "
                    + "AND   c.comp_id = cb.comp_id  "
                    + "AND   cb.comp_id = r.comp_id  "
                    + "AND   r.u_rank + c.rank_offset BETWEEN cb.min_rank AND cb.max_rank  "
                    + "AND r.u_rank BETWEEN ? AND ? ORDER BY r.comp_id, r.u_rank, r.user_id LIMIT ?;");

    public static final SQLStmt queryUserIdWithLeagueOneRank = new SQLStmt(  
            "SELECT r.user_id, r.u_rank + c.rank_offset u_rank, r.u_rank + c.rank_offset - cb.min_rank + 1  rank_in_league, r.score, cb.display_name  "
                    + "FROM competitions c "
                    + "   , competition_bands cb "
                    + "   , ratings r  "
                    + "WHERE r.comp_id = ?  "
                    + "AND   c.comp_id = cb.comp_id  "
                    + "AND   cb.comp_id = r.comp_id  "
                    + "AND   r.u_rank + c.rank_offset BETWEEN cb.min_rank AND cb.max_rank  "
                    + "AND r.u_rank = ?  "
                    + "ORDER BY r.comp_id, r.u_rank, r.user_id LIMIT ?;");

    public static final SQLStmt queryUserIdWithLeagueAndStartOneRank = new SQLStmt(
            "SELECT r.user_id, r.u_rank + c.rank_offset u_rank, r.u_rank -  cb.min_rank + 1 rank_in_league, r.score, cb.display_name  "
                    + "FROM competitions c "
                    + "   , competition_bands cb "
                    + "   , ratings r  "
                    + "WHERE r.comp_id = ?  "
                    + "AND   c.comp_id = cb.comp_id  "
                    + "AND   cb.comp_id = r.comp_id  "
                    + "AND   r.u_rank + c.rank_offset BETWEEN cb.min_rank AND cb.max_rank  "
                    + "AND r.u_rank = ? "
                    + "AND r.user_id > ? "
                    + "ORDER BY r.comp_id, r.u_rank, r.user_id LIMIT ?;");

    public static final SQLStmt getComp = new SQLStmt("SELECT * FROM competitions WHERE comp_id = ?;");

   // @formatter:on

    /**
     * Used for debugging / testing.
     */
    boolean chatty = false;

    /**
     * VoltDB procedure call to return results for a dense rank competition
     * where there are a large number (>1000) of entries. Rows are ordered by
     * rank and UserId. Note that the UserId parameter has a special purpose -
     * if you query rank 'x' and get pageSize rows back you'll need to do
     * another call to get the next pageSize rows. In this case you pass in the
     * last UserId you saw and we'll try and resume the ranking from the row
     * after. We 'try' because it's hypothetically possible for UserId 'x' to be
     * updated between calls and change ranks...
     * 
     * @param compId
     *            Competition ID
     * @param startRank
     *            start rank, starting at 1.
     * @param endRank
     *            end rank
     * @param userId
     *            user identifier. Optional. See above.
     * @param pageSize
     *            How many rows to return
     * @return A VoltTable containing the rankings.
     * @throws VoltAbortException
     */
    public VoltTable[] run(long compId, int startRank, int endRank, int userId, int pageSize)
            throws VoltAbortException {

        VoltTable[] results = null;

        // Query competition to get offset...
        voltQueueSQL(getComp, EXPECT_ONE_ROW, compId);
        VoltTable[] vtComp = voltExecuteSQL();
        vtComp[0].advanceRow();
        long evilOffset = vtComp[0].getLong("RANK_OFFSET");

        // If a userId hasn't been given to us ..
        if (userId == Integer.MIN_VALUE || userId  == 0) {

            // If we only want one rank things are simple...
            if (startRank == endRank) {
                voltQueueSQL(queryUserIdWithLeagueOneRank, compId, startRank - evilOffset, pageSize);
                results = voltExecuteSQL(true);
            } else {

                // We want data for more than one rank.
                // For performance reasons and the 'bottomless pit of data'
                // problem we do this one rank at a time...
                // Start by seeing if we can get pageSize rows for the first
                // rank...
                voltQueueSQL(queryUserIdWithLeagueOneRank, compId, startRank - evilOffset, pageSize);
                results = voltExecuteSQL();

                int rowsGot = results[0].getRowCount();

                if (rowsGot < pageSize) {
                    // We didn't get pageSize rows back for the first entry.
                    // Query the next one and add the results to the first set.
                    // Keep going until we reach endRank or have pageSize
                    // entries...
                    int nextRank = startRank + 1;

                    if (chatty) {
                        System.out.println("Got " + rowsGot + " rows from " + (startRank - evilOffset) + " need "
                                + (pageSize - rowsGot));
                    }

                    // Make our table writable...
                    results[0] = getWritableCopy(results[0]);

                    while (nextRank <= endRank && rowsGot < pageSize) {

                        voltQueueSQL(queryUserIdWithLeagueOneRank, compId, nextRank - evilOffset, pageSize - rowsGot);
                        VoltTable[] extraResults = voltExecuteSQL();
                        rowsGot += extraResults[0].getRowCount();

                        while (extraResults[0].advanceRow()) {
                            results[0].add(extraResults[0].cloneRow());
                        }

                        if (chatty) {
                            System.out.println(
                                    "Added " + extraResults[0].getRowCount() + " from " + (nextRank - evilOffset));
                        }

                        nextRank++;
                    }

                    if (chatty) {
                        System.out.println("Returning " + results[0].getRowCount());
                    }

                }

            }

        } else {

            // We have been given a userId to start from.

            // Easy case - all rows are in one rank...
            if (startRank == endRank) {
                voltQueueSQL(queryUserIdWithLeagueAndStartOneRank, compId, startRank - evilOffset, userId, pageSize);
                results = voltExecuteSQL(true);
            } else {
                // Hard case - we need to scan mumtiple ranks and start from
                // userId 'x' in the first one.
                // This is *not* the same as 'rank between a and b and userId >
                // x', so subsequent queries
                // need to ignore userId..

                voltQueueSQL(queryUserIdWithLeagueAndStartOneRank, compId, startRank - evilOffset, null, pageSize);
                results = voltExecuteSQL();

                int rowsGot = results[0].getRowCount();

                if (rowsGot < +pageSize) {
                    // We didn't get pageSize rows back for the first entry.
                    // Keep going until we reach endRank or have pageSize
                    // entries...
                    int nextRank = startRank + 1;

                    if (chatty) {
                        System.out.println("Got " + rowsGot + " rows from " + (startRank - evilOffset) + " need "
                                + (pageSize - rowsGot));
                    }

                    // Make our table writable...
                    results[0] = getWritableCopy(results[0]);

                    while (nextRank <= endRank && rowsGot < pageSize) {

                        voltQueueSQL(queryUserIdWithLeagueOneRank, compId, nextRank - evilOffset, pageSize - rowsGot);
                        VoltTable[] extraResults = voltExecuteSQL();
                        rowsGot += extraResults[0].getRowCount();

                        while (extraResults[0].advanceRow()) {
                            results[0].add(extraResults[0].cloneRow());
                        }

                        if (chatty) {
                            System.out.println(
                                    "Added " + extraResults[0].getRowCount() + " from " + (nextRank - evilOffset));
                        }

                        nextRank++;
                    }

                    if (chatty) {
                        System.out.println("Returning " + results[0].getRowCount());
                    }

                }

            }
        }

        return results;

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
