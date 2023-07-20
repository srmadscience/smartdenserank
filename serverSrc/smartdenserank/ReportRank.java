package smartdenserank;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class ReportRank extends VoltProcedure {

    public static final SQLStmt getRowsForRankOne = new SQLStmt("SELECT r.comp_id, r.user_id,r.score, r.u_rank "
            + "FROM ratings r, competitions c " + "WHERE c.comp_id = ? " + "AND r.comp_id = c.comp_id "
            + "AND r.u_rank = (1 - c.rank_offset) " + "ORDER BY r.user_id LIMIT 1;");

    public static final SQLStmt getRowsForUser = new SQLStmt(
            "SELECT * FROM ratings WHERE comp_id = ? AND user_id = ?;");

    public static final SQLStmt getMaxRank = new SQLStmt("SELECT max(u_rank) u_rank from ratings where comp_id = ?;");

    public static final SQLStmt getMinScore = new SQLStmt("SELECT min(score) score FROM ratings WHERE comp_id = ?;");

    public static final SQLStmt addRow = new SQLStmt(
            "INSERT INTO ratings (comp_id, user_id,score  ,u_rank) VALUES (?,?,?,?);");

    public static final SQLStmt getTotalRowsForScore = new SQLStmt(
            "SELECT how_many FROM rows_per_score_view WHERE comp_id = ? AND score = ?;");

    public static final SQLStmt updRankAndScore = new SQLStmt(
            "UPDATE ratings SET u_rank = ?, score = ? WHERE comp_id = ? AND user_id = ?;");

    public static final SQLStmt shuffleRanksDown = new SQLStmt(
            "UPDATE ratings SET u_rank = u_rank + 1 WHERE comp_id = ? AND score < ?;");

    public static final SQLStmt shuffleRanksUp = new SQLStmt(
            "UPDATE ratings SET u_rank = u_rank - 1 WHERE comp_id = ? AND score < ?;");

    public static final SQLStmt getComp = new SQLStmt("SELECT * FROM competitions WHERE comp_id = ?;");

    public static final SQLStmt updComp = new SQLStmt("UPDATE competitions SET max_u_rank = ? WHERE comp_id = ?;");
    public static final SQLStmt updCompOffset = new SQLStmt(
            "UPDATE competitions SET rank_offset = ? WHERE comp_id = ?;");

    /**
     * Used for debugging / testing.
     */
    private boolean chatty = false;

    /**
     * Used for debugging / testing.
     */

    private boolean chattyShuffle = true;

    /**
     * Used for debugging / testing.
     */
    int hits = 0;

    public VoltTable[] run(long compId, int userId) throws VoltAbortException {

        hits++;

        if (chatty) {
            System.out.println(System.lineSeparator() + compId + " " + userId);
        }

        voltQueueSQL(getRowsForRankOne, compId);
        voltQueueSQL(getRowsForUser, compId, userId);
        voltQueueSQL(getComp, EXPECT_ONE_ROW, compId);
        voltQueueSQL(getMinScore, compId);

        VoltTable[] vt1 = voltExecuteSQL();

        vt1[2].advanceRow();
        long maxRank = vt1[2].getLong("MAX_U_RANK");
        long evilOffset = vt1[2].getLong("RANK_OFFSET");

        if (vt1[0].getRowCount() == 0) {
            // No rows exist - add the first one
            if (chatty) {
                System.out.println("Add first");
            }
            voltQueueSQL(addRow, compId, userId, 1, 1);
        } else {

            if (vt1[1].getRowCount() == 0) {

                // We don't have a row for this user id. Find max rank and
                // use that
                vt1[3].advanceRow();

                long minScore = vt1[3].getLong("SCORE");

                if (chatty) {
                    System.out.println("Add to bottom");
                }

                if (minScore == 1) {
                    voltQueueSQL(addRow, compId, userId, 1, maxRank);
                } else {
                    voltQueueSQL(addRow, compId, userId, 1, maxRank + 1);
                    voltQueueSQL(updComp, maxRank + 1, compId);
                }

            } else {

                // user id exists in table...get current score/rank
                vt1[1].advanceRow();

                long myRank = vt1[1].getLong("U_RANK");
                long myScore = vt1[1].getLong("SCORE");

                // See if a record for myScore + 1 exists. Also see how many
                // people have my score..

                voltQueueSQL(getTotalRowsForScore, compId, myScore);
                voltQueueSQL(getTotalRowsForScore, compId, myScore + 1);
                VoltTable[] vt2 = voltExecuteSQL();

                long totalForMyScore = 0;
                long totalForMyScorePlusOne = 0;

                if (vt2[0].advanceRow()) {
                    totalForMyScore = vt2[0].getLong("HOW_MANY");
                }

                if (vt2[1].advanceRow()) {
                    totalForMyScorePlusOne = vt2[1].getLong("HOW_MANY");
                }

                if (totalForMyScorePlusOne > 0) {
                    // If so, increment our rank so it moves to that bucket
                    // System.out.println("Move to " + (myRank - 1));
                    voltQueueSQL(updRankAndScore, myRank - 1, myScore + 1, compId, userId);

                    // Have we just emptied all the rows for this score? If so,
                    // promote all
                    // lower ranks...
                    if (totalForMyScore <= 1) {
                        if (chattyShuffle) {
                            System.out.println(compId + " " + userId + ": Shuffle Up - Rank " + myRank
                                    + " empty. Hits since shuffle: " + hits);
                            hits = 0;
                        }
                        voltQueueSQL(shuffleRanksUp, compId, myScore);

                        voltQueueSQL(updComp, maxRank - 1, compId);
                    }

                } else {

                    // Nobody has myscore + 1..

                    if (chatty) {
                        System.out.println("inc score");
                    }

                    // If more than one row had 'myscore' move all rows below
                    // down one rank...
                    if (totalForMyScore > 1) {

                        boolean isTop = false;

                        if (myRank == (1 - evilOffset)) {
                            isTop = true;
                        }

                        if (chattyShuffle) {
                            System.out.println(compId + " " + userId + ": shuffle down - Rank " + myRank
                                    + " split. Hits since shuffle: " + hits + ". isTop=" + isTop);
                            hits = 0;
                        }

                        if (isTop) {

                            voltQueueSQL(updRankAndScore, 0 - evilOffset, myScore + 1, compId, userId);
                            evilOffset++;
                            // voltQueueSQL(shuffleRanksDown, compId, myScore +
                            // 1);
                            voltQueueSQL(updCompOffset, evilOffset, compId);

                        } else {

                            // So we leave our rank alone, inc score to myscore
                            // + 1
                            voltQueueSQL(updRankAndScore, myRank, myScore + 1, compId, userId);
                            voltQueueSQL(shuffleRanksDown, compId, myScore + 1);
                            voltQueueSQL(updComp, maxRank + 1, compId);
                        }
                    }

                }

            }

        }

        long startMs = System.currentTimeMillis();
        VoltTable[] results = voltExecuteSQL(true);

        if ((chattyShuffle || chatty) && System.currentTimeMillis() - startMs > 4) {
            System.out.println("ms=" + (System.currentTimeMillis() - startMs));
        }

        return results;

    }
}
