package ca.dal.csci6405.project.mrjob02;

import java.io.*;
import java.util.*;

public class MyUtils {
    public static int M = 5, WIDTH = 6;
    public static long BIT( int k ) { return 1L<<k; }
    public static long MASK( int k ) { return BIT(k)-1L; }
    public static int getPos( long u ) {
        for ( int i = 0; i < M; ++i )
            if ( ((u>>(i*WIDTH)) & MASK(WIDTH)) != 0 )
                return i;
        return -1;
    }
}
