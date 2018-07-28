package model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource{
    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:/home/sachet/eclipse-workspace/Musicre/"+DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUMS_ID = "_id";
    public static final String COLUMN_ALBUMS_NAME = "name";
    public static final String COLUMN_ALBUMS_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTISTS_ID = "_id";
    public static final String COLUMN_ARTISTS_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONGS_ID = "_id";
    public static final String COLUMN_SONGS_TRACK = "track";
    public static final String COLUMN_SONGS_TITLE = "title";
    public static final String COLUMN_SONGS_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ARTIST = 4;

    public static final int ORDER_BY_NONE = 0;
    public static final int ORDER_BY_ASC = 1;
    public static final int ORDER_BY_DESC = 2;

    public static final String QUERY_ALBUMS_BY_ARTIST = "select "+
    TABLE_ALBUMS+"."+COLUMN_ALBUMS_NAME +" from " +TABLE_ALBUMS+" inner join "+TABLE_ARTISTS+ " on "+
    TABLE_ALBUMS+"."+COLUMN_ALBUMS_ARTIST+ "="+TABLE_ARTISTS+"."+COLUMN_ARTISTS_ID+" where "+ TABLE_ARTISTS+
    "."+ COLUMN_ARTISTS_NAME+"=\"";

    public static final String QUERY_ALBUMS_BY_ARTIST_SORT = " order by "+TABLE_ALBUMS+ "."+COLUMN_ALBUMS_NAME+ " collate nocase ";

    public static final String QUERY_ARTISTS_BY_SONGS =
            "select "+ TABLE_ARTISTS+"."+COLUMN_ARTISTS_NAME+", "+TABLE_ALBUMS+"."+COLUMN_ALBUMS_NAME+", "
            +TABLE_SONGS+"."+COLUMN_SONGS_TRACK+" from "+TABLE_SONGS+" inner join "+TABLE_ALBUMS+" on "
            +TABLE_SONGS+"."+COLUMN_SONGS_ALBUM+" = "+TABLE_ALBUMS+"."+COLUMN_ALBUMS_ID+" inner join "
            +TABLE_ARTISTS+" on "+TABLE_ALBUMS+"."+COLUMN_ALBUMS_ARTIST+" = "+TABLE_ARTISTS+"."+COLUMN_ARTISTS_ID
            +" where "+TABLE_SONGS+"."+COLUMN_SONGS_TITLE+" =\"";

    public static final String QUERY_ARTISTS_BY_SONGS_SORT =
            " order by "+TABLE_ARTISTS+"."+COLUMN_ARTISTS_NAME+", "+TABLE_ALBUMS+"."+COLUMN_ALBUMS_NAME+" collate nocase ";

    public static final String INSERT_ARTIST = "insert into "+TABLE_ARTISTS+
            '('+COLUMN_ARTISTS_NAME+")values(?)";
    public static final String INSERT_ALBUM = "insert into "+TABLE_ALBUMS+
            '('+COLUMN_ALBUMS_NAME+", "+COLUMN_ALBUMS_ARTIST+")values(?,?)";
    public static final String INSERT_SONG = "insert into "+TABLE_SONGS+
            '('+COLUMN_SONGS_TRACK+", "+COLUMN_SONGS_TITLE+", "+COLUMN_SONGS_ALBUM+")values(?,?,?)";
    /*create view if not exists artist_list as select  artists.name as artist,albums.name as album,
    songs.track,songs.title from songs inner join albums on songs.album = albums._id inner join artists on
    albums.artist = artists._id order by artists.name, albums.name, songs.track*/

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";

    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "create view if not exists "+TABLE_ARTIST_SONG_VIEW+
            " as select "+TABLE_ARTISTS+"."+COLUMN_ARTISTS_NAME+" as artist"+", "+TABLE_ALBUMS+"."+COLUMN_ALBUMS_NAME+" as album"
            +", "+TABLE_SONGS+"."+COLUMN_SONGS_TRACK+", "+TABLE_SONGS+"."+COLUMN_SONGS_TITLE+" from "+TABLE_SONGS+" inner join "
            +TABLE_ALBUMS+" on "+TABLE_SONGS+"."+COLUMN_SONGS_ALBUM+" = "+TABLE_ALBUMS+"."+COLUMN_ALBUMS_ID+" inner join "
            +TABLE_ARTISTS+" on "+TABLE_ALBUMS+"."+COLUMN_ALBUMS_ARTIST+" = "+TABLE_ARTISTS+"."+COLUMN_ARTISTS_ID+" order by "
            +TABLE_ARTISTS+"."+COLUMN_ARTISTS_NAME+", "+TABLE_ALBUMS+"."+COLUMN_ALBUMS_NAME+", "+TABLE_SONGS+"."+COLUMN_SONGS_TRACK;
//    select artist, album, track from artist_list where title = "Go Your Own Way";
    public static final String QUERY_VIEW_SONG_INFO = "select "+COLUMN_ALBUMS_ARTIST+", "
        +COLUMN_SONGS_ALBUM+", "+COLUMN_SONGS_TRACK+" from "+TABLE_ARTIST_SONG_VIEW+" where "
        +COLUMN_SONGS_TITLE+"=\"";

    public static final String QUERY_VIEW_SONG_PREP = "select "+COLUMN_ALBUMS_ARTIST+", "
            +COLUMN_SONGS_ALBUM+", "+COLUMN_SONGS_TRACK+" from "+TABLE_ARTIST_SONG_VIEW+" where "
            +COLUMN_SONGS_TITLE+"=?";

    public static final String QUERY_ARTISTS = "select "+COLUMN_ARTISTS_ID+" from "
            +TABLE_ARTISTS+" where "+COLUMN_ARTISTS_NAME+" =?";

    public static final String QUERY_ALBUMS = "select "+COLUMN_ALBUMS_ID+" from "
            +TABLE_ALBUMS+" where "+COLUMN_ALBUMS_NAME+" =?";

    public static final String QUERY_SONGS = "select "+COLUMN_SONGS_ID+" from "
            +TABLE_SONGS+" where "+COLUMN_SONGS_TITLE+" =?";

    private Connection conn;
    private PreparedStatement querySongInfoView;

    private PreparedStatement insertIntoArtist;
    private PreparedStatement insertIntoAlbum;
    private PreparedStatement insertIntoSong;
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    private PreparedStatement querySong;

    public boolean open(){
        try{
            conn = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfoView = conn.prepareStatement(QUERY_VIEW_SONG_PREP);
            insertIntoArtist = conn.prepareStatement(INSERT_ARTIST,Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbum = conn.prepareStatement(INSERT_ALBUM,Statement.RETURN_GENERATED_KEYS);
            insertIntoSong = conn.prepareStatement(INSERT_SONG);
            queryArtist = conn.prepareStatement(QUERY_ARTISTS);
            queryAlbum = conn.prepareStatement(QUERY_ALBUMS);
            querySong = conn.prepareStatement(QUERY_SONGS);
            return true;
        }catch(SQLException e){
            System.out.println("There is something wrong: "+e.getMessage());
            return false;
        }
    }
    public void close(){
        try{
            if(querySongInfoView!=null){querySongInfoView.close();}
            if(insertIntoArtist!=null){insertIntoArtist.close();}
            if(insertIntoAlbum!=null){insertIntoAlbum.close();}
            if(insertIntoSong!=null){insertIntoSong.close();}
            if(queryArtist!=null){queryArtist.close();}
            if(queryAlbum!=null){queryAlbum.close();}
            if(conn!=null){
                conn.close();
            }
        }catch(SQLException e){
            System.out.println("Couldn't close the database: "+e.getMessage());
        }
    }
    public List<Artist> queryArtist(int sortorder){
        StringBuilder sb = new StringBuilder("select * from artists ");
        if(sortorder!=ORDER_BY_NONE){
            sb.append("order by ");
            sb.append(COLUMN_ARTISTS_NAME);
            sb.append(" collate nocase ");
            if(sortorder == ORDER_BY_DESC) sb.append("desc");
            else sb.append("asc");
        }
        List<Artist> artlist = new ArrayList<>();
        try(Statement statement = conn.createStatement();
        ResultSet results = statement.executeQuery(sb.toString())){
            while(results.next()){
                Artist art = new Artist();
                art.set_id(results.getInt(INDEX_ARTIST_ID));
                art.setName(results.getString(INDEX_ARTIST_NAME));
                artlist.add(art);
            }
            return artlist;
        }catch(SQLException e){
            return null;
        }
    }
    public List<String> queryAlbumsForArtist(String artistName, int sortOrder){
        //select albums.name from albums inner join artists on albums.artist = artists._id
        // where artists.name = "Iron Maiden" order by albums.name collate nocase asc;
        List<String> ress = new ArrayList<>();
        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST);
        sb.append(artistName+"\"");
        if(sortOrder!=ORDER_BY_NONE){
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if(sortOrder == ORDER_BY_DESC)sb.append("desc");
            else sb.append("asc");
        }
        try(Statement statement = conn.createStatement();
        ResultSet results = statement.executeQuery(sb.toString())){
            while(results.next()){
                ress.add(results.getString(1));
            }
            return ress;
        }catch(SQLException e){
            System.out.println(e.getMessage());
            return null;
        }
    }
    public List<SongArtist> querySongsForArtists(String songName, int sortOrder){
        List<SongArtist> ress = new ArrayList<>();
        StringBuilder sb = new StringBuilder(QUERY_ARTISTS_BY_SONGS);
        sb.append(songName+"\"");
        if(sortOrder!=ORDER_BY_NONE){
            sb.append(QUERY_ARTISTS_BY_SONGS_SORT);
            if(sortOrder == ORDER_BY_DESC)sb.append("desc");
            else sb.append("asc");
        }
        try(Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery(sb.toString())){
            while(result.next()){
                SongArtist sng = new SongArtist();
                sng.setArtistName(result.getString(1));
                sng.setAlbumName(result.getString(2));
                sng.setTrack(result.getInt(3));
                ress.add(sng);
            }
            return ress;
        }catch(SQLException e){
            System.out.println(e.getMessage());
            return null;
        }
    }
    public void querySongMetaData(){
        String sql = "select * from "+TABLE_SONGS;
        try(Statement statement = conn.createStatement();
        ResultSet results = statement.executeQuery(sql)){
            ResultSetMetaData metadata = results.getMetaData();
            int numCount = metadata.getColumnCount();
            for(int i=1; i<=numCount; i++){
                System.out.println("The column "+i+" has name:"+metadata.getColumnName(i));
                System.out.println();
            }
        }catch(SQLException e){
            System.out.println("Query Failed "+e.getMessage());
        }
    }
    public void getCount(String tableName){
        String sql ="select count(*) as count from "+tableName;
        try(Statement statement = conn.createStatement();
            ResultSet results = statement.executeQuery(sql)){
            /*The result that is returned due to the execution of the statement is form of columns*/
            int count = results.getInt("count");
            System.out.format("Count = %d",count);
        }catch(SQLException e){
            System.out.println("Query failed "+e.getMessage());
            return ;
        }
    }
    public boolean createView(){
        try(Statement statement = conn.createStatement()){
            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;
        }catch(Exception e){
            System.out.println(e.getMessage());
            return false;
        }
    }
    public List<SongArtist> queryView(String songTitle){
    /*All parameters in JDBC are represented by the ? symbol, which is known as the parameter marker. You must supply values
     for every parameter before executing the SQL statement.*/
    /*The setXXX() methods bind values to the parameters, where XXX represents the Java data type of the value you wish to bind
    to the input parameter. If you forget to supply the values, you will receive an SQLException.*/
    /*Each parameter marker is referred by its ordinal position. The first marker represents position 1, the next position 2,
    and so forth. This method differs from that of Java array indices, which starts at 0.*/
    /*All of the Statement object's methods for interacting with the database (a) execute(), (b) executeQuery(), and (c)
    executeUpdate() also work with the PreparedStatement object. However, the methods are modified to use SQL statements
    that can input the parameters.*/

    try{
        List<SongArtist> lst = new ArrayList();
        querySongInfoView.setString(1,songTitle);
        ResultSet results = querySongInfoView.executeQuery();
        while(results.next()){
            SongArtist sngart = new SongArtist();
            sngart.setArtistName(results.getString(1));
            sngart.setAlbumName(results.getString(2));
            sngart.setTrack(results.getInt(3));
            lst.add(sngart);
        }
        return lst;
    }catch(SQLException e){
        System.out.println(e.getMessage());
        return null;
    }

        /*try(Statement statement = conn.createStatement();
            ResultSet results = statement.executeQuery(sb.toString())){
            while(results.next()){
                SongArtist sngart = new SongArtist();
                sngart.setArtistName(results.getString(1));
                sngart.setAlbumName(results.getString(2));
                sngart.setTrack(results.getInt(3));
                lst.add(sngart);
            }
            return lst;
        }catch(SQLException e){
            System.out.println(e.getMessage());
            return null;
        }*/
    }
    private int insertArtist(String name) throws SQLException{
        queryArtist.setString(1,name);
        ResultSet results = queryArtist.executeQuery();
        if(results.next()){
            return results.getInt(1);
        }else{
            insertIntoArtist.setString(1,name);
            int affectedRows = insertIntoArtist.executeUpdate();//->returns the number of rows affected by executing the sql
            //statement
            if(affectedRows!=1) throw new SQLException("Couldn't load artist into artist table");
            ResultSet generatedKeys = insertIntoArtist.getGeneratedKeys();
            if(generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else{
                throw new SQLException("Couldn't get _id for the artist");
            }
        }
    }
    private int insertAlbum(String name, int artistId) throws SQLException{
        queryAlbum.setString(1,name);
        ResultSet results = queryAlbum.executeQuery();
        if(results.next()){
            return results.getInt(1);
        }else{
            insertIntoAlbum.setString(1,name);
            insertIntoAlbum.setInt(2,artistId);
            int affectedRow = insertIntoAlbum.executeUpdate();
            if(affectedRow!=1) throw new SQLException("Couldn't load album int the table");
            ResultSet generatedKeys = insertIntoAlbum.getGeneratedKeys();
            if(generatedKeys.next()){return generatedKeys.getInt(1);}
            else{throw new SQLException("Couldn't get _id for album");}
        }
    }
    public void insertSong(String title, String artist, String album, int track){
        try{
            querySong.setString(1,title);
            ResultSet resuts = querySong.executeQuery();
            if(resuts.next()){
                System.out.println("The song is already there");
                return;
            }
            conn.setAutoCommit(false);
            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album,artistId);
            insertIntoSong.setInt(1,track);
            insertIntoSong.setString(2,title);
            insertIntoSong.setInt(8,albumId);
            int affectedRows = insertIntoSong.executeUpdate();
            if(affectedRows == 1){
                conn.commit();
            }else{
                throw new SQLException("The Song insert failed");
            }
        }catch(Exception e){
            System.out.println("Insert Song exception "+e.getMessage());
            try{
                System.out.println("Performing rollback");
                conn.rollback();
            }catch(SQLException e1){
                System.out.println("Something's is really bad");
            }
        }finally{
            try{
                System.out.println("Resetting default auto commit behaviour");
                conn.setAutoCommit(true);
            }catch(SQLException e){
                System.out.println("Couldn't reset auto-commit "+e.getMessage());
            }
        }
    }
}
