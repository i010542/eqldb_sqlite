    var lang = 'en';
    switch_lang();
    document.getElementById("data").focus();
    time0 = new Date().getTime();
    dbname = "nobeloscars";
    var query = document.location.toString().split("?");
    if (query.length>1){
        dbname = query[1]
    }
    document.getElementById("db").value = dbname;
    check_dbname();

    function check_dbname(){
        dbname = document.getElementById("db").value;
        if (dbname == 'nobeloscars') {
            document.getElementById("dbname").innerText = "诺贝尔奖和奥斯卡奖知识库";
        } else if (dbname == 'davinci_8e') {
            document.getElementById("dbname").innerText = "达芬奇世界知识库";
        } else {
            document.getElementById("dbname").innerText = "　";
        }
    }

    function switch_lang(){
        clear_answer();
        if (lang=='zh') {
            lang = 'en';
            document.getElementById("switch").innerText = 'English';
            document.getElementById("demo").value = 'Example';
            document.getElementById("run").value = 'Query';
            document.getElementById("p0").innerText = 'Explore the knowledge base s:p:o(q:v)';
            document.getElementById("p1").innerText = 'award received';
            document.getElementById("p2").innerText = 'place of birth';
            document.getElementById("p3").innerText = 'country of citizenship';
            document.getElementById("p4").innerText = 'instance of';
            document.getElementById("p5").innerText = 'author';
            document.getElementById("p6").innerText = 'occupation';
            document.getElementById("p7").innerText = 'area';
            document.getElementById("p8").innerText = 'child';
            document.getElementById("p9").innerText = 'native language';
        } else if (lang=='en'){
            lang = 'zh';
            document.getElementById("switch").innerText = '中文';
            document.getElementById("demo").value = '示例';
            document.getElementById("run").value = 'Q一下';
            document.getElementById("p0").innerText = '探索知识库s:p:o(q:v)';
            document.getElementById("p1").innerText = '所获奖项';
            document.getElementById("p2").innerText = '出生地';
            document.getElementById("p3").innerText = '国籍';
            document.getElementById("p4").innerText = '性质';
            document.getElementById("p5").innerText = '作者';
            document.getElementById("p6").innerText = '职业';
            document.getElementById("p7").innerText = '面积';
            document.getElementById("p8").innerText = '子女';
            document.getElementById("p9").innerText = '母语';
        }
    }

    function fill_eql(id){
        document.getElementById("data").value = "?x:" + document.getElementById(id).innerText + ":?y";
        clear_answer();
    }

    var changing = false;
    function input_change(){
        var selection = window.getSelection ? window.getSelection() : document.selection;
        var range = selection.createRange ? selection.createRange() : selection.getRangeAt(0);
        document.getElementById("data").value = range.endOffset;
        window._range = range;
        start = range.startOffset;
        end = range.endOffset;

        clear_answer();
        div = document.getElementById("input");
    }

    function clear_input() {
        document.getElementById("data").value = "";
        clear_answer();
    }

    function clear_answer() {
        istart = -1;
        document.getElementById("progress").innerText = "";
        document.getElementById("answer").innerText = "";
    }

    var eql_id = '';
    var istart = -1;
    var token = 'anonymous';
    var total = 0;
    function get_result(){
        if (istart>=0) {
            xmlhttp = new XMLHttpRequest();
            url = encodeURI("/eql/" + eql_id +"/result/"+istart.toString(10));
            xmlhttp.open("GET",url,false);
            xmlhttp.send();
            result = xmlhttp.responseText;
            json = JSON.parse(result);
            div_answer = document.getElementById("answer");
            div_progress = document.getElementById("progress");
            for (var i=0; i<json.length; i++){
                n = Number(json[i].n);
                if (n==0)
                    percent = "running"
                else
                    percent = ((Number(json[i].i))*100/n).toFixed(2) + "％";
                if (json[i].s == "invalid") {
                    div_progress.innerText = json[i].s + "，连接被终止,或没有权限";
                    istart = -1;
                    logout();
                    token = "anonymous";
                    break;
                } else if (json[i].s == "done") {
                    duration = (new Date().getTime()-time0)/1000;
                    div_progress.innerText = json[i].s + ", 完成, " + total.toString() +"条结果，耗时 " + duration.toString() + 's';
                    istart = -1;
                    logout();
                    token = "anonymous";
                    break;
                } else if (json[i].s == "running"){
                    div_progress.innerText = json[i].e + '：' + percent;
                    istart ++;
                } else if (json[i].s == "error"){
                    div_progress.innerText = json[i].s + "," + json[i].r + ",请检查高亮部分";
                    istart = -1;
                    logout();
                    token = "anonymous";
                    json2 = JSON.parse(json[i].r);
                    error_pos = json2.pos
                    var input = document.getElementById ("data");
                    input.selectionStart = error_pos;
                    input.selectionEnd = -1;
                    input.focus();
                } else if (json[i].s == "output") {
                    n = Number(json[i].n);
                    if (n==0) n = 1;
                    div_progress.innerText = json[i].e + ": " + percent;
                    if (json[i].r.length >2) {
                        jr = JSON.parse(json[i].r)
                        s = ""
                        for (var i2=0; i2<jr.length; i2++){
                            v = jr[i2].var
                            if (v != '?factID') {
                                if (v.length > 1 && v[1] != 't') {
                                    s = s + jr[i2].var.slice(1) + '=';
                                }
                                s = s + jr[i2].label + "　";
                            }
                        }
                        div_answer.innerText = div_answer.innerText + "\n" + s;
                        total = total + 1;
                    }
                    istart ++;
                }
            }
        }
    }

    function login(){
        usr = document.getElementById("usr").value;
        pwd = document.getElementById("pwd").value;
        if ( (usr != "") && (pwd != "")){
            xmlhttp = new XMLHttpRequest();
            url = encodeURI("/token/" + usr + "/" + pwd);
            xmlhttp.open("GET", url, false);
            xmlhttp.send();
            result = xmlhttp.responseText;
            json = JSON.parse(result);
            token = json.token;
        }
    }

    function logout() {
        try {
                xmlhttp = new XMLHttpRequest();
                url = encodeURI("/token/" + token);
                xmlhttp.open("DELETE", url, false);
                xmlhttp.send();
                result = xmlhttp.responseText;
                json = JSON.parse(result);
                accepted = json.accepted == "true";
        } catch (e) {
        }
    }

    function run_eql2() {
        time0 = new Date().getTime();
        clear_answer();
        login();
        ta = document.getElementById("data");
        data = ta.value;
        xmlhttp = new XMLHttpRequest();
        url = encodeURI("/eql/" + dbname + "/" + lang + "/" + token);
        var formData = new FormData();
        formData.append('q', data);
        xmlhttp.open("POST", url, false);
        xmlhttp.send(formData);
        result = xmlhttp.responseText;
        json = JSON.parse(result);
        eql_id = json.eql_id;
        istart = 0;
        total = 0;
        get_result();
        setInterval(get_result,1000);
    }

    var eqls_zh = new Array(
    "萧伯纳:？:都柏林",
    "?:所获奖项:诺贝尔文学奖（日期:1945）",
    "萧伯纳:出生地:？",
    "萧伯纳:所获奖项:诺贝尔文学奖（獎金:？）",
    "萧伯纳:所获奖项:奥斯卡最佳改编剧本奖(?)",
    "？:所获奖项:诺贝尔文学奖",
    "？:所获奖项:诺贝尔文学奖 \\unlimit",
    "？:所获奖项:诺贝尔文学奖 \\limit 10",
    "?x:子女(\\repeat:0+):萧伯纳",
    "?x:所获奖项:诺贝尔文学奖(日期:?y)\
        \\order by ?y desc",
    "?x:所获奖项:?y(日期:1925,奖金:?z)",
    "?x:所获奖项:诺贝尔文学奖(日期:?y)\
        \\group by ?x.国籍\
        \\order by ?y asc",
    "?x:所获奖项:诺贝尔文学奖(日期:?y)\
        \\filter ?x.国籍=美国\
        \\filter ?y>1940年",
    "?x:所获奖项:诺贝尔文学奖(日期:?y)\
        \\filter ?x \\match %威廉%",
    "（？x:所获奖项）:性质:诺贝尔奖\
        \\and\
        (？x:所获奖项）:性质:奥斯卡金像奖",
    "？x:职业:作家\
        \\and\
        ？x:出生地:都柏林",
    "(?x:所获奖项):性质:诺贝尔奖\
        \\and\
        (?x:所获奖项):性质:奥斯卡金像奖，\
        ?y=count(?x)",
    "(?x:所获奖项):性质:诺贝尔奖\
        \\and\
        (?x:所获奖项):性质:奥斯卡金像奖，\
        ?x:所获奖项:?y(獎金:?z1）\
        \\and\
        ?y:性质:诺贝尔奖，\
        ？z2=avg(?z1)，\
        ？z3=max(?z1) ",
    " George Bernard Shaw:出生地:?",
    "(?:child):place of birth:New York",
    " ?x:职业:音乐家\
        \\and\
        ?x:子女:?y\
        \\and\
        (count(?y of ?x)>=2),\
        ?z=count(?y of ?x),\
        ANS ?x,?x.母语，?z",
    " ?x:性质:美国州份，\
        ?x:面积:?y,\
        ?z=max(?y),\
        ANS ?z"
    );

    var eqls_en = new Array(
    "George Bernard Shaw:？:Dublin",
    "?:award received:Nobel Prize in Literature（point in time:1945）",
    "George Bernard Shaw:place of birth:？",
    "George Bernard Shaw:award received:Nobel Prize in Literature（prize money:？）",
    "George Bernard Shaw:award received:Academy Award for Best Writing\\, Adapted Screenplay(?)",
    "？:award received:Nobel Prize in Literature",
    "?x:award received:Nobel Prize in Literature(point in time:?y)\
        \\order by ?y desc",
    "?x:award received:Nobel Prize in Literature(point in time:?y)\
        \\group by ?x.country of citizenship\
        \\order by ?y asc",
    "?x:award received:Nobel Prize in Literature(point in time:?y)\
        \\filter ?x.country of citizenship=United States of America\
        \\filter ?y>1940",
    "?x:award received:Nobel Prize in Literature(point in time:?y)\
        \\filter ?x \\match %William%",
    "（？x:award received）: instance of:Nobel Prize\
        \\and\
        (？x:award received）: instance of:Academy Awards",
    "？x:occupation:writer_person who uses written words to communicate ideas and to produce works of literature\
        \\and\
        ？x:place of birth:Dublin",
    "(?x:award received): instance of:Nobel Prize\
        \\and\
        (?x:award received): instance of:Academy Awards，\
        ?y=count(?x)",
    "(?x:award received): instance of:Nobel Prize\
        \\and\
        (?x:award received): instance of:Academy Awards，\
        ?x:award received:?y(prize money:?z1）\
        \\and\
        ?y: instance of:Nobel Prize，\
        ？z2=avg(?z1)，\
        ？z3=max(?z1) ",
    " George Bernard Shaw:place of birth:?",
    "(?:child):place of birth:New York",
    " ?x:occupation:musician\
        \\and\
        ?x:child:?y\
        \\and\
        (count(?y of ?x)>=2),\
        ?z=count(?y of ?x),\
        ANS ?x,?x.native language，?z",
    " ?x: instance of:state of the United States，\
        ?x:area:?y,\
        ?z=max(?y),\
        ANS ?z"
    );
    var idemo=0;
    function fill_demo() {
        if (lang=='zh') {
            if (idemo >= eqls_zh.length ) {
                idemo = 0;
            }
            document.getElementById("data").value = eqls_zh[idemo];
        }
        if (lang=='en') {
            if (idemo >= eqls_en.length ) {
                idemo = 0;
            }
            document.getElementById("data").value = eqls_en[idemo];
        }
        clear_answer();
        idemo += 1;
    }
