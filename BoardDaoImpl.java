package casestudy.dataaccess;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import casestudy.business.domain.Board;
import casestudy.business.service.BoardDao;

public class BoardDaoImpl implements BoardDao {
	private DataSource dataSource;
	
	public BoardDaoImpl() {
		try {
			Context context = new InitialContext();
			context = (Context) context.lookup("java:comp/env");
			dataSource = (DataSource) context.lookup("jdbc/dukeboardDB");
		} catch (NamingException ne) {
			System.err.println("JNDI error occured.");
			ne.printStackTrace(System.err);
			throw new RuntimeException("JNDI error occured." + ne.getMessage());
		}
		System.out.println("branch test");
	}

	private Connection obtainConnection() throws SQLException {
		return dataSource.getConnection();
	}

	@Override
	public List<Board> selectBoardList(Map<String, Object> searchInfo) {
		// 1. searchInfo로 부터 검색 조건을 구한다.
		String searchType = (String) searchInfo.get("searchType");
		String searchText = (String) searchInfo.get("searchText");
		//1.2. searchInfo Map으로부터 현재 페이지에 보여질 게시글의 행 번호(startRow, endRow) 값을 구한다.
		int skipCount = (Integer) searchInfo.get("startRow") - 1;
		int rowCount = (Integer) searchInfo.get("endRow") - skipCount;
		
		// 2. searchType 값에 따라 사용될 조건절(WHERE)을 생성한다.
		String whereSQL = "";
		if ((searchType == null) || (searchType.length() == 0)) {
			whereSQL = "";
		} else if (searchType.equals("all")) {
			whereSQL = " WHERE title LIKE ? OR writer LIKE ? OR contents LIKE ?";
		} else if ((searchType.equals("title")) || (searchType.equals("writer")) || (searchType.equals("contents"))) {
			whereSQL = " WHERE " + searchType + " LIKE ?";
		}

		// 3. LIKE 정에 포함될 수 있도록 searchText 값 앞뒤에 % 기호를 붙인다.
		if (searchText != null) {
			searchText = "%" + searchText.trim() + "%";
		}
		
		String query = "SELECT num, writer, title, read_count, reg_date FROM board" + whereSQL
				+ " ORDER BY num DESC LIMIT ?, ?";
		System.out.println("BoardDaoImpl selectBoardList() query: " + query);

		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Board> temp = new ArrayList<Board>();
		Board board = null;

		try {
			connection = obtainConnection();
			pstmt = connection.prepareStatement(query);
			
			if ((searchType == null) || (searchType.length() == 0)) {
				pstmt.setInt(1, skipCount);
				pstmt.setInt(2, rowCount);
			} else if (searchType.equals("all")) {
				pstmt.setString(1, searchText);
				pstmt.setString(2, searchText);
				pstmt.setString(3, searchText);
				pstmt.setInt(4, skipCount);
				pstmt.setInt(5, rowCount);
			} else if ((searchType.equals("title")) | (searchType.equals("writer")) | (searchType.equals("contents"))) {
				pstmt.setString(1, searchText);
				pstmt.setInt(2, skipCount);
				pstmt.setInt(3, rowCount);
			}
			
			rs = pstmt.executeQuery();

			String title;
			while (rs.next()) {
				title = rs.getString("title");
				if(title.length()>28) {
					title = new StringBuilder(title.substring(0, 28)).append("...").toString();
				}
				
				board = new Board(rs.getInt("num"),
										rs.getString("writer"),
										title,
										rs.getInt("read_count"),
										rs.getString("reg_date"));
				temp.add(board);
			}

		} catch (SQLException se) {
			System.err.println("BoardDaoImpl selectBoardList() Error :" + se.getMessage());
			se.printStackTrace(System.err);
			throw new RuntimeException("A database error occurred. " + se.getMessage());

		} finally {
			try { if(rs != null) rs.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (connection != null) connection.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
		}

		return temp;
	}

	@Override
	public int selectBoardCount(Map<String, Object> searchInfo) {
		// 1. searchInfo로 부터 검색 조건을 구한다.
		String searchType = (String) searchInfo.get("searchType");
		String searchText = (String) searchInfo.get("searchText");
		
		// 2. searchType 값에 따라 사용될 조건절(WHERE)을 생성한다.
		String whereSQL = "";
		if ((searchType == null) || (searchType.length() == 0)) {
			whereSQL = "";
		} else if (searchType.equals("all")) {
			whereSQL = " WHERE title LIKE ? OR writer LIKE ? OR contents LIKE ?";
		} else if ((searchType.equals("title")) | (searchType.equals("writer")) | (searchType.equals("contents"))) {
			whereSQL = " WHERE " + searchType + " LIKE ?";
		}		

		// 3. LIKE 정에 포함될 수 있도록 searchText 값 앞뒤에 % 기호를 붙인다.
		if (searchText != null) {
			searchText = "%" + searchText.trim() + "%";
		}
		
		String query = "SELECT count(num) FROM board " + whereSQL;
		System.out.println("BoardDaoImpl selectBoardCount() query: " + query);

		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;
		try {
			connection = obtainConnection();
			pstmt = connection.prepareStatement(query);
			
			if ((searchType == null) || (searchType.length() == 0)) {
			} else if (searchType.equals("all")) {
				pstmt.setString(1, searchText);
				pstmt.setString(2, searchText);
				pstmt.setString(3, searchText);
			} else if ((searchType.equals("title")) | (searchType.equals("writer")) | (searchType.equals("contents"))) {
				pstmt.setString(1, searchText);
			}
			
			rs = pstmt.executeQuery();

			if (rs.next()) {
				count = rs.getInt("count(num)");
			}

		} catch (SQLException se) {
			System.err.println("BoardDaoImpl selectBoardCount() Error :" + se.getMessage());
			se.printStackTrace(System.err);
			throw new RuntimeException("A database error occurred. " + se.getMessage());

		} finally {
			try { if(rs != null) rs.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (connection != null) connection.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
		}

		return count;
	}

	@Override
	public Board selectBoard(int num) {
		Board board = null;

		String query = "SELECT num, writer, title, contents, ip, read_count, reg_date, mod_date FROM board WHERE num=?";
		System.out.println("BoardDaoImpl selectBoard() query: " + query);

		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;


		try {
			connection = obtainConnection();
			pstmt = connection.prepareStatement(query);
			
			pstmt.setInt(1, num);
			
			rs = pstmt.executeQuery();

			if (rs.next()) {
				board = new Board(rs.getInt("num"),
						rs.getString("writer"),
						rs.getString("title"),
						rs.getString("contents"),
						rs.getString("ip"),
						rs.getInt("read_count"),
						rs.getString("reg_date"),
						rs.getString("mod_date"));
			}

		} catch (SQLException se) {
			System.err.println("BoardDaoImpl selectBoard() Error :" + se.getMessage());
			se.printStackTrace(System.err);
			throw new RuntimeException("A database error occurred. " + se.getMessage());

		} finally {
			try { if(rs != null) rs.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (connection != null) connection.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
		}

		return board;
	}

	@Override
	public void addReadCount(int num) {
		String query = "UPDATE board SET read_count=read_count+1 WHERE num=?";
		System.out.println("BoardDaoImpl addReadCount() query: " + query);

		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = obtainConnection();
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, num);
			pstmt.executeUpdate();

		} catch (SQLException se) {
			System.err.println("BoardDaoImpl addReadCount() Error :" + se.getMessage());
			se.printStackTrace(System.err);
			throw new RuntimeException("A database error occurred. " + se.getMessage());

		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (connection != null) connection.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
		}
	}

	@Override
	public boolean boardNumExists(int num) {
		boolean result = false;

		String query = "SELECT num FROM board WHERE num=?";

		System.out.println("BoardDaoImpl boardNumExists() query: " + query);

		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			connection = obtainConnection();
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, num);
			rs = pstmt.executeQuery();
			
			result = rs.next();

		} catch (SQLException se) {
			System.err.println("BoardDaoImpl boardNumExists() Error :" + se.getMessage());
			se.printStackTrace(System.err);
			throw new RuntimeException("A database error occurred. " + se.getMessage());

		} finally {
			try { if(rs != null) rs.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (connection != null) connection.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
		}

		return result;
	}

	@Override
	public void insertBoard(Board board) {
		String query = "INSERT INTO board (writer, title, contents, ip, read_count, reg_date, mod_date) VALUES (?, ?, ?, ?, 0, NOW(), NOW())";
		System.out.println("BoardDaoImpl insertBoard() query: " + query);

		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = obtainConnection();
			pstmt = connection.prepareStatement(query);
			pstmt.setString(1, board.getWriter());
			pstmt.setString(2, board.getTitle());
			pstmt.setString(3, board.getContents());
			pstmt.setString(4, board.getIp());
			pstmt.executeUpdate();

		} catch (SQLException se) {
			System.err.println("BoardDaoImpl insertBoard() Error :" + se.getMessage());
			se.printStackTrace(System.err);
			throw new RuntimeException("A database error occurred. " + se.getMessage());

		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (connection != null) connection.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
		}
	}

	@Override
	public void updateBoard(Board board) {
		String query = "UPDATE board SET writer=?, title=?, contents=?, ip=?, mod_date=NOW() WHERE num=?";
		System.out.println("BoardDaoImpl updateBoard() query: " + query);

		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = obtainConnection();
			pstmt = connection.prepareStatement(query);
			pstmt.setString(1, board.getWriter());
			pstmt.setString(2, board.getTitle());
			pstmt.setString(3, board.getContents());
			pstmt.setString(4, board.getIp());
			pstmt.setInt(5, board.getNum());
			pstmt.executeUpdate();

		} catch (SQLException se) {
			System.err.println("BoardDaoImpl updateBoard() Error :" + se.getMessage());
			se.printStackTrace(System.err);
			throw new RuntimeException("A database error occurred. " + se.getMessage());

		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (connection != null) connection.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
		}
	}

	@Override
	public void deleteBoard(int num) {
		String query = "DELETE FROM board WHERE num = ?";
		System.out.println("BoardDaoImpl deleteBoard() query: " + query);

		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = obtainConnection();
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, num);
			pstmt.executeUpdate();

		} catch (SQLException se) {
			System.err.println("BoardDaoImpl deleteBoard() Error :" + se.getMessage());
			se.printStackTrace(System.err);
			throw new RuntimeException("A database error occurred. " + se.getMessage());

		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
			try { if (connection != null) connection.close(); } catch (SQLException ex) { ex.printStackTrace(System.err); }
		}
	}

}
