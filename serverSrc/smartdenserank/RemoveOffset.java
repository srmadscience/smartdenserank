package smartdenserank;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

public class RemoveOffset extends VoltProcedure {

    public static final SQLStmt getComp = new SQLStmt("SELECT * FROM competitions WHERE comp_id = ?;");
    public static final SQLStmt updCompOffset = new SQLStmt(
            "UPDATE competitions SET rank_offset = ?, max_u_rank = max_u_rank + ? WHERE comp_id = ?;");
    public static final SQLStmt fixRankNumber = new SQLStmt(
            "UPDATE ratings SET u_rank = u_rank + ? WHERE comp_id = ?;");


    boolean chattyShuffle = true;
    
    public VoltTable[] run(long compId) throws VoltAbortException {

        voltQueueSQL(getComp, EXPECT_ONE_ROW, compId);
        VoltTable[] t = voltExecuteSQL();
        t[0].advanceRow();
        long evilOffset = t[0].getLong("RANK_OFFSET");

        if (evilOffset != 0) {
            
            voltQueueSQL(updCompOffset, 0, evilOffset, compId);
            voltQueueSQL(fixRankNumber, evilOffset, compId);
            this.setAppStatusString("Removed offset of " + evilOffset);
            if (chattyShuffle) {
                System.out.println("Removed offset of " + evilOffset);
            }
       
        } else {
            this.setAppStatusString("Offset is 0");
        }

        long startMs = System.currentTimeMillis();
        VoltTable[] results = voltExecuteSQL(true);

        if (chattyShuffle  && System.currentTimeMillis() - startMs > 4) {
            System.out.println("ms=" + (System.currentTimeMillis() - startMs));
        }

        return results;

    }
}
