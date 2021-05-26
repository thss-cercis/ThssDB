package cn.edu.thssdb.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Cells {
  private List<Cell> cells = new ArrayList<>();

  public static Cells fromCell(Cell... cs) {
    Cells ret = new Cells();
    ret.cells.addAll(Arrays.asList(cs));
    return ret;
  }
}
