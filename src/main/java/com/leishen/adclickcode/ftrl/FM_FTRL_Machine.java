package com.leishen.adclickcode.ftrl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.LoggingMXBean;

import org.omg.PortableServer.THREAD_POLICY_ID;

import com.mysql.jdbc.jmx.LoadBalanceConnectionGroupManager;

public class FM_FTRL_Machine {
    String name; // 模型的在线学习类的名称
    private float alpha;
    private float beta;
    private float L1;
    private float L2;
    private float alpha_fm;
    private float beta_fm;
    private float L1_fm;
    private float L2_fm;
    private int fm_dim;
    private float fm_initDev;
    private float dropoutRate;
    private int D; // 初始化的数组的大小
    private int n_epochs; // 循环的次数

    private double[] n;
    private double[] z;
    private double[] w;

    private Map<Integer, double[]> n_fm;
    private Map<Integer, double[]> z_fm;
    private Map<Integer, double[]> w_fm;

    // 参数文件
    private String w_filePath;
    private String z_filePath;
    private String n_filePath;
    private String w_fm_filePath;
    private String z_fm_filePath;
    private String n_fm_filePath;
    private String emptyfilePath;

	/*
     * 这是在线学习类的初始化函数, name代表在线学习实例的名称, parameterlist代表在线学习实例所需要的参数
	 *
	 */

    public FM_FTRL_Machine(String name, List<Object> parameterList) {
        this.alpha = Float.parseFloat(parameterList.get(0).toString());
        this.beta = Float.parseFloat(parameterList.get(1).toString());
        this.L1 = Float.parseFloat(parameterList.get(2).toString());
        this.L2 = Float.parseFloat(parameterList.get(3).toString());
        this.alpha_fm = Float.parseFloat(parameterList.get(4).toString());
        this.beta_fm = Float.parseFloat(parameterList.get(5).toString());
        this.L1_fm = Float.parseFloat(parameterList.get(6).toString());
        this.L2_fm = Float.parseFloat(parameterList.get(7).toString());
        this.fm_dim = Integer.parseInt(parameterList.get(8).toString());
        this.fm_initDev = Float.parseFloat(parameterList.get(9).toString());
        this.dropoutRate = Float.parseFloat(parameterList.get(10).toString());
        this.name = name;
        this.D = Integer.parseInt(parameterList.get(11).toString());
        this.n_epochs = Integer.parseInt(parameterList.get(12).toString());

        this.n = new double[this.D];
        this.z = new double[this.D];
        this.w = new double[this.D];

        this.n_fm = new HashMap<Integer, double[]>();
        this.z_fm = new HashMap<Integer, double[]>();
        this.w_fm = new HashMap<Integer, double[]>();

    }

    private void init_I(int i) {
        if (!this.w_fm.containsKey(i)) {
            double[] value = new double[this.fm_dim];
            this.n_fm.put(i, value);
            this.z_fm.put(i, value);
            this.w_fm.put(i, value);
        }
    }

    private void init_FM(int i) {
        if (!this.w_fm.containsKey(i)) {
            double[] valuen = new double[this.fm_dim];
            double[] valuez = new double[this.fm_dim];
            double[] valuew = new double[this.fm_dim];

            this.n_fm.put(i, valuen);
            this.z_fm.put(i, valuez);
            this.w_fm.put(i, valuew);
        }
        Random random = new Random();
        BigDecimal b = new BigDecimal(random.nextGaussian());
        for (int z = 0; z < this.fm_dim; z++) {
            // 初始高斯分布值
            this.z_fm.get(i)[z] = Math.abs(b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue());
        }
    }

    // 每次训练之前，查看参数文件是否存在，不存在则创建。
    public void initUseFilePath() {
        this.w_filePath = ".\\Resource\\" + this.name + "\\w_file.txt";
        this.z_filePath = ".\\Resource\\" + this.name + "\\z_file.txt";
        this.n_filePath = ".\\Resource\\" + this.name + "\\n_file.txt";
        this.w_fm_filePath = ".\\Resource\\" + this.name + "\\w_fm_file.txt";
        this.z_fm_filePath = ".\\Resource\\" + this.name + "\\z_fm_file.txt";
        this.n_fm_filePath = ".\\Resource\\" + this.name + "\\n_fm_file.txt";
        this.emptyfilePath = ".\\Resource\\" + this.name + "\\empty.txt";

        // learn学习器的文件夹，里面放置参数文件
        File learnDir = new File(".\\Resource\\" + this.name);
        if (!learnDir.exists()) {
            learnDir.mkdirs();
        }
        File w_file = new File(this.w_filePath);
        File z_file = new File(this.z_filePath);
        File n_file = new File(this.n_filePath);
        File w_fm_file = new File(this.w_fm_filePath);
        File z_fm_file = new File(this.z_fm_filePath);
        File n_fm_file = new File(this.n_fm_filePath);
        File emptyfile = new File(this.emptyfilePath);

        try {
            if (!w_file.exists())
                w_file.createNewFile();

            if (!z_file.exists())
                z_file.createNewFile();

            if (!n_file.exists())
                n_file.createNewFile();

            if (!w_fm_file.exists())
                w_fm_file.createNewFile();
            if (!z_fm_file.exists())
                z_fm_file.createNewFile();
            if (!n_fm_file.exists())
                n_fm_file.createNewFile();
            if (!emptyfile.exists()) {
                emptyfile.createNewFile();
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    // 开始计算预测值
    public double predict_raw(List<Integer> dataIndexs) {


        double raw_y = 0;
        this.w[0] = ((-z[0]) / ((beta + Math.sqrt(n[0])) / alpha));
        raw_y += this.w[0];
        for (int index : dataIndexs) {

            int sign = z[index] < 0 ? -1 : 1;
            if (sign * z[index] <= L1)
                this.w[index] = 0;
            else
                this.w[index] = (sign * L1 - z[(int) index]) / ((beta + Math.sqrt(n[(int) index])) / alpha + L2);

            raw_y += this.w[index];
        }

        for (int index : dataIndexs) {
            this.init_FM(index);
            for (int index_fm_dim = 0; index_fm_dim < this.fm_dim; index_fm_dim++) {
                int sign = z_fm.get(index)[index_fm_dim] < 0 ? -1 : 1;

                if (sign * z_fm.get(index)[index_fm_dim] <= this.L1_fm) {
                    w_fm.get(index)[index_fm_dim] = 0;

                } else {
                    w_fm.get(index)[index_fm_dim] = ((sign * this.L1_fm - z_fm.get(index)[index_fm_dim])
                            / ((beta_fm + Math.sqrt(n_fm.get(index)[index_fm_dim])) / (alpha_fm + L2_fm)));
                }

            }
        }

        for (int length = 0; length < dataIndexs.size(); length++) {
            for (int J = length + 1; J < dataIndexs.size(); J++) {
                for (int q = 0; q < this.fm_dim; q++) {
                    raw_y += w_fm.get(dataIndexs.get(length))[q] * w_fm.get(dataIndexs.get(J))[q];
                }
            }
        }

        BigDecimal b = new BigDecimal(raw_y);
        return b.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();

    }

    public void update(List<Integer> dataIndexs, double p, double y) {

        double[] z_array = this.z;

        Map<Integer, double[]> n_fm_map = this.n_fm;
        Map<Integer, double[]> w_fm_map = this.w_fm;
        Map<Integer, double[]> z_fm_map = this.z_fm;

        double errorValue = 0;
        Map<Integer, double[]> fm_sum = new HashMap<Integer, double[]>();
        // 异常数据预测值和实际值得误差，因为异常数据很少，所以权重给大一点
        if (y == 1) {
            errorValue = (p - y) * 1.2;
        } else
            errorValue = p - y;
        int length = dataIndexs.size();
        dataIndexs.add(0);

        for (int index : dataIndexs) {
            double sigma = (Math.sqrt(n[index] + errorValue * errorValue) - Math.sqrt(n[index])) / this.alpha;
            this.z[index] += errorValue - sigma * this.w[index];
            this.n[index] += errorValue * errorValue;

            BigDecimal b = new BigDecimal(this.z[index]);
            BigDecimal c = new BigDecimal(this.n[index]);

            this.z[index] = b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
            this.n[index] = c.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
            double array[] = new double[this.fm_dim];
            fm_sum.put(index, array);

        }
        Iterator<Integer> iter = dataIndexs.iterator();
        while (iter.hasNext()) {
            int s = iter.next();
            if (s == 0) {
                iter.remove();
            }
        }

        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                if (i != j) {
                    for (int k = 0; k < this.fm_dim; k++) {
                        fm_sum.get(dataIndexs.get(i))[k] += w_fm.get(dataIndexs.get(j))[k];
                    }
                }
            }
        }

        for (int index : dataIndexs) {
            for (int k = 0; k < this.fm_dim; k++) {
                double g_fm = errorValue * fm_sum.get(index)[k];
                double sigma = (Math.sqrt(n_fm.get(index)[k] + g_fm * g_fm) - Math.sqrt(n_fm.get(index)[k]))
                        / this.alpha_fm;

                this.z_fm.get(index)[k] += g_fm - sigma * w_fm.get(index)[k];
                this.n_fm.get(index)[k] += g_fm * g_fm;

                BigDecimal b = new BigDecimal(this.z_fm.get(index)[k]);
                BigDecimal c = new BigDecimal(this.n_fm.get(index)[k]);

                this.z_fm.get(index)[k] = b.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();

                this.n_fm.get(index)[k] = c.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
            }
        }

    }

    private void createFile(String filePath) {
        File newFile = new File(filePath);
        if (!newFile.exists()) {
            try {
                newFile.createNewFile();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void emptyFile() {
        File w_file = new File(this.w_filePath);
        File z_file = new File(this.z_filePath);
        File n_file = new File(this.n_filePath);
        File w_fm_file = new File(this.w_fm_filePath);
        File z_fm_file = new File(this.z_fm_filePath);
        File n_fm_file = new File(this.n_fm_filePath);


        this.emptyOneFile(w_file);
        this.emptyOneFile(z_file);
        this.emptyOneFile(n_file);

        this.emptyOneFile(w_fm_file);
        this.emptyOneFile(z_fm_file);
        this.emptyOneFile(n_fm_file);

    }

    private void emptyOneFile(File file) {

        try {
            FileWriter fw5 = new FileWriter(file);
            BufferedWriter bw1 = new BufferedWriter(fw5);
            bw1.write("");
            bw1.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void copyFile() {
        this.createFile(".\\Resource\\" + this.name + "\\n_file_copy.txt");
        this.createFile(".\\Resource\\" + this.name + "\\w_file_copy.txt");
        this.createFile(".\\Resource\\" + this.name + "\\z_file_copy.txt");
        this.createFile(".\\Resource\\" + this.name + "\\n_fm_file_copy.txt");
        this.createFile(".\\Resource\\" + this.name + "\\z_fm_file_copy.txt");
        this.createFile(".\\Resource\\" + this.name + "\\w_fm_file_copy.txt");

        this.copyOneFile(this.n_filePath, ".\\Resource\\" + this.name + "\\n_file_copy.txt");
        this.copyOneFile(this.w_filePath, ".\\Resource\\" + this.name + "\\w_file_copy.txt");
        this.copyOneFile(this.z_filePath, ".\\Resource\\" + this.name + "\\z_file_copy.txt");
        this.copyOneFile(this.n_fm_filePath, ".\\Resource\\" + this.name + "\\n_fm_file_copy.txt");
        this.copyOneFile(this.z_fm_filePath, ".\\Resource\\" + this.name + "\\z_fm_file_copy.txt");
        this.copyOneFile(this.w_fm_filePath, ".\\Resource\\" + this.name + "\\w_fm_file_copy.txt");
    }

    private void copyOneFile(String af, String bf) {
        FileInputStream is = null;
        FileOutputStream os = null;

        File input = new File(af);
        File output = new File(bf);
        try {
            is = new FileInputStream(input);
            os = new FileOutputStream(output);
            byte b[] = new byte[1024];
            int len;
            try {
                len = is.read(b);
                while (len != -1) {
                    os.write(b, 0, len);
                    len = is.read(b);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (is != null)
                    is.close();
                if (os != null)
                    os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void writeString(String filePath, String content) {
        File file = new File(filePath);
        FileWriter fileWritter;
        try {
            fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.write(content);
            bufferWritter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void write_w() {
        File file = new File(this.w_filePath);
        if (!file.exists())
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        FileWriter fileWritter;
        try {
            fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            for (int i = 0; i < this.w.length; i++) {
                BigDecimal b = new BigDecimal(this.w[i]);
                bufferWritter.write(i + "," + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue() + "\r\n");
            }

            bufferWritter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void write_n() {
        File file = new File(this.n_filePath);
        if (!file.exists())
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        FileWriter fileWritter;
        try {
            fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            for (int i = 0; i < this.n.length; i++) {
                BigDecimal b = new BigDecimal(this.n[i]);
                bufferWritter.write(i + "," + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue() + "\r\n");
            }

            bufferWritter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void write_z() {
        File file = new File(this.z_filePath);
        if (!file.exists())
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        FileWriter fileWritter;
        try {
            fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            for (int i = 0; i < this.z.length; i++) {
                BigDecimal b = new BigDecimal(this.z[i]);
                bufferWritter.write(i + "," + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue() + "\r\n");
            }

            bufferWritter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void write_w_fm() {
        File file = new File(this.w_fm_filePath);
        if (!file.exists())
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        FileWriter fileWritter;
        try {
            fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            for (int key : this.w_fm.keySet()) {
                String value = "";
                for (int z = 0; z < this.fm_dim - 1; z++) {
                    BigDecimal b = new BigDecimal(Math.abs(this.w_fm.get(key)[z]));
                    value = value + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue() + ",";
                }
                BigDecimal b = new BigDecimal(Math.abs(this.w_fm.get(key)[this.fm_dim - 1]));
                value = value + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                bufferWritter.write(key + "," + value + "\r\n");

            }

            bufferWritter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void write_z_fm() {
        File file = new File(this.z_fm_filePath);
        if (!file.exists())
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        FileWriter fileWritter;
        try {
            fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            for (int key : this.z_fm.keySet()) {
                String value = "";
                for (int z = 0; z < this.fm_dim - 1; z++) {
                    BigDecimal b = new BigDecimal(Math.abs(this.z_fm.get(key)[z]));

                    value = value + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue() + ",";
                }
                BigDecimal b = new BigDecimal(Math.abs(this.z_fm.get(key)[this.fm_dim - 1]));
                value = value + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

                bufferWritter.write(key + "," + value + "\r\n");

            }

            bufferWritter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void write_n_fm() {
        File file = new File(this.n_fm_filePath);
        if (!file.exists())
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        FileWriter fileWritter;
        try {
            fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            for (int key : this.n_fm.keySet()) {
                String value = "";
                for (int z = 0; z < this.fm_dim - 1; z++) {
                    BigDecimal b = new BigDecimal(Math.abs(this.n_fm.get(key)[z]));

                    value = value + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue() + ",";
                }
                BigDecimal b = new BigDecimal(Math.abs(this.n_fm.get(key)[this.fm_dim - 1]));
                value = value + b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                bufferWritter.write(key + "," + value + "\r\n");

            }

            bufferWritter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public double predict(List<Integer> dataIndexs) {
        return 1.0 / (1. + Math.exp(-Math.max(Math.min(this.predict_raw(dataIndexs), 35.0), -35.0)));
    }


    public double logLoss(double p, double y) {
        double px = Math.max(Math.min(p, 1.0 - 1e-15), 1e-15);
        double x = y == 1 ? -Math.log(px) : -Math.log(1 - px);
        return x;

    }


    // 加载上次训练完之后文件中的相应参数，接着新的训练
    public void loadFMAndWParameters() {
        this.copyFile();
        File wfile = new File(this.w_filePath);
        File zfile = new File(this.z_filePath);
        File nfile = new File(this.n_filePath);
        File wfm_file = new File(this.w_fm_filePath);
        File zfm_file = new File(this.z_fm_filePath);
        File nfm_file = new File(this.n_fm_filePath);
        try {
            BufferedReader w = new BufferedReader(new FileReader(wfile));
            BufferedReader z = new BufferedReader(new FileReader(zfile));
            BufferedReader n = new BufferedReader(new FileReader(nfile));

            BufferedReader wfm = new BufferedReader(new FileReader(wfm_file));
            BufferedReader zfm = new BufferedReader(new FileReader(zfm_file));
            BufferedReader nfm = new BufferedReader(new FileReader(nfm_file));

            String value = "";
            while ((value = w.readLine()) != null) {
                String[] splitValue = value.trim().split(",");
                this.w[Integer.parseInt(splitValue[0])] = Float.parseFloat(splitValue[1]);
            }
            w.close();

            while ((value = z.readLine()) != null) {
                String[] splitValue = value.trim().split(",");
                this.z[Integer.parseInt(splitValue[0])] = Float.parseFloat(splitValue[1]);
            }
            z.close();

            while ((value = n.readLine()) != null) {
                String[] splitValue = value.trim().split(",");
                this.n[Integer.parseInt(splitValue[0])] = Float.parseFloat(splitValue[1]);
            }
            n.close();

            while ((value = wfm.readLine()) != null) {

                String[] splitValue = value.trim().split(",");
                this.init_FM(Integer.parseInt(splitValue[0]));
                for (int zindex = 0; zindex < this.fm_dim; zindex++) {
                    this.w_fm.get(Integer.parseInt(splitValue[0]))[zindex] = Float.parseFloat(splitValue[zindex + 1]);
                }

            }
            wfm.close();

            while ((value = zfm.readLine()) != null) {
                String[] splitValue = value.trim().split(",");
                for (int zindex = 0; zindex < this.fm_dim; zindex++) {
                    this.init_FM(Integer.parseInt(splitValue[0]));
                    this.z_fm.get(Integer.parseInt(splitValue[0]))[zindex] = Float.parseFloat(splitValue[zindex + 1]);
                }

            }
            zfm.close();

            while ((value = nfm.readLine()) != null) {
                String[] splitValue = value.trim().split(",");
                for (int zindex = 0; zindex < this.fm_dim; zindex++) {
                    this.n_fm.get(Integer.parseInt(splitValue[0]))[zindex] = Float.parseFloat(splitValue[zindex + 1]);
                }

            }
            nfm.close();


        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
