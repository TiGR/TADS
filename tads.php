<?php

class tads {

    protected $storagePath = "";
    protected $formats = array();
    protected $types = array("data" => ".dat", "index" => ".idx", "format" => ".fmt", "lock" => ".lock");

    public function __construct($storagePath = "", $mode = 0666) {
        if ($storagePath) {
            $this->storagePath = $storagePath
                . ($storagePath{strlen($storagePath)-1} != '/' ? '/' : '');
        } else {
            $this->storagePath = dirname(__FILE__) . "/data/";
        }

        if (!is_dir($this->storagePath)) {
            mkdir($this->storagePath, $mode, true);
        }

    }

    public function selectByID($file, $id) {
        if (!$this->_idExists($file, $id)) {
            return false;
        }
        $record = $this->_getRecordOffsets($file, $id);
        $handle = fopen($this->_getFile($file), "r");
        fseek($handle, $record['offset']);
        $data = fread($handle, $record['size']);
        fclose($handle);

        return $this->_unpack($file, $data);
    }

    public function selectRange($file, $offset, $limit, $order = "ASC") {
        if (strtoupper($order) == "ASC") {
            $id = $offset + 1;
            $direction = 1;
        } else {
            $id = $this->count($file) - $offset;
            $direction = -1;
        }

        if (!$this->_idExists($file, $id)) {
            return false;
        }

        if ($offset + $limit > $this->count($file)) {
            $limit = $this->count($file) - $offset;
        }

        for(; $limit > 0; $limit--) {
            $records[$id] = $this->selectById($file, $id);
            $id = $id + $direction;
        }
        return $records;
    }

    public function insert($file, $data) {
        $data = $this->_pack($file, $data);

        $handle = fopen($this->_getFile($file, "index"), "a");
        fwrite($handle, pack("L", filesize($this->_getFile($file))));
        fclose($handle);

        $handle = fopen($this->_getFile($file), "a");
        fwrite($handle, $data . "\n");
        fclose($handle);

        return $this->count($file);
    }

    public function update($file, $id, $data) {
        if (!$this->_idOkay($file, $id)) {
            return false;
        }
        $data = $this->_pack($file, $data);
        $offsets = $this->_getRecordOffsets($file, $id);
        if (strlen($data) > $offsets['size']) {
            $this->_expand($file, $offsets['offset']+$offsets['size'],
                strlen($data)-$offsets['size']);
        } else if (strlen($data) < $offsets['size']) {
            $this->_shrink($file, $offsets['offset']+$offsets['size'],
                $offsets['size']-strlen($data));
        }
        $handle = fopen($this->_getFile($file), "r+");
        fseek($handle, $offsets['offset']);
        fwrite($handle, $data);
        fclose($handle);
        $this->_refreshIndex($file, $id);
        return true;
    }

    public function deleteById($file, $id) {
        if (!$this->_idOkay($file, $id)) {
            return false;
        }
        $offsets = $this->_getRecordOffsets($file, $id);
        $this->_shrink($file, $offsets['offset']+$offsets['size']+1,
            $offsets['size']+1);
        $this->_refreshIndex($file, $this->count($file, $id) == $id ? $id-1 : $id);
        return true;
    }

    public function createFile($filename, $format) {
        $format = array('names' => array_keys($format), 'types' => $format);
        $handle = fopen($this->_getFile($filename, "format"), "w");
        fwrite($handle, serialize($format));
        fclose($handle);
        $this->truncate($filename);
    }

    public function truncate($file) {
        $this->_truncate($file, "data");
        $this->_truncate($file, "index");
    }

    public function dropFile($filename) {
        foreach ($this->types as $type=>$ext) {
            if (file_exists($this->_getFile($filename, $type))) {
                unlink($this->_getFile($filename, $type));
            }
        }
    }

    public function getFileFormat($file) {
        return isset($this->formats[$file]) ? $this->formats[$file]
            : $this->formats[$file] =
                unserialize(file_get_contents($this->_getFile($file, "format")));
    }

    public function count($file) {
        $count = floor(filesize($this->_getFile($file, "index")) / 4);
        return $count;
    }

    protected function _refreshIndex($file, $startId = 1) {
        if (!$this->_idOkay($file, $startId)) {
            $startId=1;
        }

        $filesize = filesize($this->_getFile($file));
        $idxFile = fopen($this->_getFile($file, "index"), "r+b");
        $datFile = fopen($this->_getFile($file), "rb");

        if ($startId == 1) {
            ftruncate($idxFile, 0);
            fwrite($idxFile, pack("L", 0));
            $offset = 0;
        } else {
            $record = $this->_getRecordOffsets($file, $startId);
            $offset = $record['offset'];
            if ($offset+$record['size'] > $filesize) {
                // index corrupt
                die('index corrupt');

                $this->_truncate($file, "index");
                $this->_refreshIndex($file, 1);
            }
            ftruncate($idxFile, $startId*4);
        }

        fseek($idxFile, 0, SEEK_END);
        fseek($datFile, $offset);

        while (!feof($datFile)) {
            fgets($datFile, 1048576);
            if (ftell($datFile)<$filesize)
                fwrite($idxFile, pack("L", ftell($datFile)));
        }

        fclose($idxFile);
        fclose($datFile);
    }

    protected function _getRecordOffsets($file, $id) {
        $handle = fopen($this->_getFile($file, "index"), "r");
        fseek($handle, ($id - 1) * 4);
        if ($id == $this->count($file, $id)) {
            $record = unpack("Loffset", fread($handle, 4));
            $record['size'] = filesize($this->_getFile($file, "data"))
                - $record['offset'];
        } else {
            $record = unpack("Loffset/Lsize", fread($handle, 8));
            $record['size'] -= $record['offset'];
        }
        fclose($handle);
        $record['size']--; // so that we won't include trailing newline.
        return $record;
    }

    protected function _idExists($file, $id) {
        if ($id > $this->count($file) or $id < 1) {
            trigger_error("TADS: bad record ID [" . intval($id)
                . "]. Maximum is " . $this->count($file),
                E_USER_WARNING);
            return false;
        }
        return true;
    }

    protected function _getFile($file, $type = "data") {
        $filename = $this->storagePath . basename($file) . $this->types[$type];
        if ($type == "index" and !file_exists($filename)) {
            touch($filename);
            $this->_refreshIndex($file);
        }
        return $filename;
    }

    protected function _pack($file, $data) {
        // packs array, containing data to single string, with array values
        // serialized and separated by tab sign.
        $format = $this->getFileFormat($file);
        $newdata = array();

        if (count($format['names']) < count($data)) {
            trigger_error("TADS: bad data - too much fields for '$file'.",
                E_USER_WARNING);
        }
        foreach ($format['names'] as $key => $name) {
            $value = isset($data[$name]) ? $data[$name] :
                (isset($data[$key]) ? $data[$key] : null);
            switch($format['types'][$name]) {
                case "int":
                    $newdata[$name] = (int) $value;
                    break;
                case "string":
                    $newdata[$name] = $this->_escape($value);
                    break;
                default:
                    $newdata[$name] = $this->_escape(serialize($value));
                    break;
            }
        }

        return implode("\t", $newdata);
    }

    protected function _unpack($file, $data) {
        // unpacks string to array, adding keys according to file format.
        $format = $this->getFileFormat($file);
        $newdata = array();
        $data = $this->_unescape($data);
        foreach (explode("\t", $data) as $key => $val) {
            switch($format['types'][$format['names'][$key]]) {
                case "int":
                    $newdata[$format['names'][$key]] = (int) $val;
                    break;
                case "string":
                    $newdata[$format['names'][$key]] = $val;
                    break;
                default:
                    $newdata[$format['names'][$key]] = unserialize($val);
                    break;
            }
        }
        return $newdata;
    }

    protected function _escape($data) {
        return str_replace(array("\\", "\t", "\r", "\n"),
            array('\x5c', '\t', '\r', '\n'), $data);
    }

    protected function _unescape($data) {
        return str_replace(array('\t', '\r', '\n', '\x5c'),
            array("\t", "\r", "\n", "\\"), $data);
    }

    protected function _truncate($file, $type) {
        fclose(fopen($this->_getFile($file, $type), "w"));
    }

    protected function _expand($file, $start, $length) {
        $block = 8192;
        $handle = fopen($this->_getFile($file), "r+b");
        $size = filesize($this->_getFile($file));
        $offset = $size-$block;
        while ($offset+$block>$start) {
            if ($offset<$start) {
                $block -= $start-$offset;
                $offset = $start;
            }
            fseek($handle, $offset);
            $buffer = fread($handle, $block);
            fseek($handle, $offset+$length);
            fwrite($handle, $buffer);
            $offset -= $block;
        }
        fclose($handle);
    }

    protected function _shrink($file, $start, $length) {
        $block = 8192;
        $handle = fopen($this->_getFile($file), "r+b");
        $size = filesize($this->_getFile($file));
        $offset = $start;
        while ($offset<$size) {
            if ($offset+$block>$size) {
                $block -= $size-$offset;
            }
            fseek($handle, $offset);
            $buffer = fread($handle, $block);
            fseek($handle, $offset-$length);
            fwrite($handle, $buffer);
            $offset += $block;
        }
        ftruncate($handle, $size-$length);
        fclose($handle);
    }

    protected function _idOkay($file, &$id) {
        $id = (int) $id;
        return ($id >= 1) and ($id <= $this->count($file, $id));
    }

}

?>
