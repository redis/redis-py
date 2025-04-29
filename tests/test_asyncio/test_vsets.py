import json
import random
import numpy as np
import pytest
import pytest_asyncio
import redis
from redis.commands.vectorset.commands import QuantizationOptions

from tests.conftest import (
    skip_if_server_version_lt,
)


@pytest_asyncio.fixture()
async def d_client(create_redis, redis_url):
    return await create_redis(url=redis_url, decode_responses=True)


@pytest_asyncio.fixture()
async def client(create_redis, redis_url):
    return await create_redis(url=redis_url, decode_responses=False)


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_with_values(d_client):
    float_array = [1, 4.32, 0.11]
    resp = await d_client.vset().vadd("myset", float_array, "elem1")
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem1")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    with pytest.raises(redis.DataError):
        await d_client.vset().vadd("myset_invalid_data", None, "elem1")

    with pytest.raises(redis.DataError):
        await d_client.vset().vadd("myset_invalid_data", [12, 45], None, reduce_dim=3)


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_with_vector(d_client):
    float_array = [1, 4.32, 0.11]
    # Convert the list of floats to a byte array in fp32 format
    byte_array = _to_fp32_blob_array(float_array)
    resp = await d_client.vset().vadd("myset", byte_array, "elem1")
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem1")
    assert _validate_quantization(float_array, emb, tolerance=0.1)


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_reduced_dim(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9]
    resp = await d_client.vset().vadd("myset", float_array, "elem1", reduce_dim=3)
    assert resp == 1

    dim = await d_client.vset().vdim("myset")
    assert dim == 3


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_cas(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9]
    resp = await d_client.vset().vadd(
        "myset", vector=float_array, element="elem1", cas=True
    )
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem1")
    assert _validate_quantization(float_array, emb, tolerance=0.1)


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_no_quant(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9]
    resp = await d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem1",
        quantization=QuantizationOptions.NOQUANT,
    )
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem1")
    assert _validate_quantization(float_array, emb, tolerance=0.0)


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_bin_quant(d_client):
    float_array = [1, 4.32, 0.0, 0.05, -2.9]
    resp = await d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem1",
        quantization=QuantizationOptions.BIN,
    )
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem1")
    expected_array = [1, 1, -1, 1, -1]
    assert _validate_quantization(expected_array, emb, tolerance=0.0)


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_q8_quant(d_client):
    float_array = [1, 4.32, 10.0, -21, -2.9]
    resp = await d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem1",
        quantization=QuantizationOptions.BIN,
    )
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem1")
    expected_array = [1, 1, 1, -1, -1]
    assert _validate_quantization(expected_array, emb, tolerance=0.0)


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_ef(d_client):
    await d_client.vset().vadd("myset", vector=[5, 55, 65, -20, 30], element="elem1")
    await d_client.vset().vadd(
        "myset", vector=[-40, -40.32, 10.0, -4, 2.9], element="elem2"
    )

    float_array = [1, 4.32, 10.0, -21, -2.9]
    resp = await d_client.vset().vadd("myset", float_array, "elem3", ef=1)
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem3")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    sim = await d_client.vset().vsim("myset", input="elem3", with_scores=True)
    assert len(sim) == 3


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_with_attr(d_client):
    float_array = [1, 4.32, 10.0, -21, -2.9]
    attrs_dict = {"key1": "value1", "key2": "value2"}
    resp = await d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem3",
        attributes=attrs_dict,
    )
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem3")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    attr_saved = await d_client.vset().vgetattr("myset", "elem3")
    assert attr_saved == attrs_dict

    resp = await d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem4",
        attributes={},
    )
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem4")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    attr_saved = await d_client.vset().vgetattr("myset", "elem4")
    assert attr_saved is None

    resp = await d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem5",
        attributes=json.dumps(attrs_dict),
    )
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem5")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    attr_saved = await d_client.vset().vgetattr("myset", "elem5")
    assert attr_saved == attrs_dict


@skip_if_server_version_lt("7.9.0")
async def test_add_elem_with_numlinks(d_client):
    elements_count = 100
    vector_dim = 10
    for i in range(elements_count):
        float_array = [random.randint(0, 10) for x in range(vector_dim)]
        await d_client.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=8,
        )

    float_array = [1, 4.32, 0.11, 0.5, 0.9, 0.1, 0.2, 0.3, 0.4, 0.5]
    resp = await d_client.vset().vadd("myset", float_array, "elem_numlinks", numlinks=8)
    assert resp == 1

    emb = await d_client.vset().vemb("myset", "elem_numlinks")
    assert _validate_quantization(float_array, emb, tolerance=0.5)

    numlinks_all_layers = await d_client.vset().vlinks("myset", "elem_numlinks")
    for neighbours_list_for_layer in numlinks_all_layers:
        assert len(neighbours_list_for_layer) <= 8


@skip_if_server_version_lt("7.9.0")
async def test_vsim_count(d_client):
    elements_count = 30
    vector_dim = 800
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        await d_client.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=64,
        )

    vsim = await d_client.vset().vsim("myset", input="elem1")
    assert len(vsim) == 10
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], str)

    vsim = await d_client.vset().vsim("myset", input="elem1", count=5)
    assert len(vsim) == 5
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], str)

    vsim = await d_client.vset().vsim("myset", input="elem1", count=50)
    assert len(vsim) == 30
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], str)

    vsim = await d_client.vset().vsim("myset", input="elem1", count=15)
    assert len(vsim) == 15
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], str)


@skip_if_server_version_lt("7.9.0")
async def test_vsim_with_scores(d_client):
    elements_count = 20
    vector_dim = 50
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        await d_client.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=64,
        )

    vsim = await d_client.vset().vsim("myset", input="elem1", with_scores=True)
    assert len(vsim) == 10
    assert isinstance(vsim, dict)
    assert isinstance(vsim["elem1"], float)
    assert 0 <= vsim["elem1"] <= 1


@skip_if_server_version_lt("7.9.0")
async def test_vsim_with_different_vector_input_types(d_client):
    elements_count = 10
    vector_dim = 5
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        attributes = {"index": i, "elem_name": f"elem_{i}"}
        await d_client.vset().vadd(
            "myset",
            float_array,
            f"elem_{i}",
            numlinks=4,
            attributes=attributes,
        )
    sim = await d_client.vset().vsim("myset", input="elem_1")
    assert len(sim) == 10
    assert isinstance(sim, list)

    float_array = [1, 4.32, 0.0, 0.05, -2.9]
    sim_to_float_array = await d_client.vset().vsim("myset", input=float_array)
    assert len(sim_to_float_array) == 10
    assert isinstance(sim_to_float_array, list)

    fp32_vector = _to_fp32_blob_array(float_array)
    sim_to_fp32_vector = await d_client.vset().vsim("myset", input=fp32_vector)
    assert len(sim_to_fp32_vector) == 10
    assert isinstance(sim_to_fp32_vector, list)
    assert sim_to_float_array == sim_to_fp32_vector

    with pytest.raises(redis.DataError):
        await d_client.vset().vsim("myset", input=None)


@skip_if_server_version_lt("7.9.0")
async def test_vsim_unexisting(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9]
    await d_client.vset().vadd("myset", vector=float_array, element="elem1", cas=True)

    with pytest.raises(redis.ResponseError):
        await d_client.vset().vsim("myset", input="elem_not_existing")

    sim = await d_client.vset().vsim("myset_not_existing", input="elem1")
    assert sim == []


@skip_if_server_version_lt("7.9.0")
async def test_vsim_with_filter(d_client):
    elements_count = 50
    vector_dim = 800
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        attributes = {"index": i, "elem_name": f"elem_{i}"}
        await d_client.vset().vadd(
            "myset",
            float_array,
            f"elem_{i}",
            numlinks=4,
            attributes=attributes,
        )
    float_array = [-random.uniform(10, 20) for x in range(vector_dim)]
    attributes = {"index": elements_count, "elem_name": "elem_special"}
    await d_client.vset().vadd(
        "myset",
        float_array,
        "elem_special",
        numlinks=4,
        attributes=attributes,
    )
    sim = await d_client.vset().vsim("myset", input="elem_1", filter=".index > 10")
    assert len(sim) == 10
    assert isinstance(sim, list)
    for elem in sim:
        assert int(elem.split("_")[1]) > 10

    sim = await d_client.vset().vsim(
        "myset",
        input="elem_1",
        filter=".index > 10 and .index < 15 and .elem_name in ['elem_12', 'elem_17']",
    )
    assert len(sim) == 1
    assert isinstance(sim, list)
    assert sim[0] == "elem_12"

    sim = await d_client.vset().vsim(
        "myset",
        input="elem_1",
        filter=".index > 25 and .elem_name in ['elem_12', 'elem_17', 'elem_19']",
        ef=100,
    )
    assert len(sim) == 0
    assert isinstance(sim, list)

    sim = await d_client.vset().vsim(
        "myset",
        input="elem_1",
        filter=".index > 28 and .elem_name in ['elem_12', 'elem_17', 'elem_special']",
        filter_ef=1,
    )
    assert len(sim) == 0, (
        f"Expected 0 results, but got {len(sim)} with filter_ef=1, sim: {sim}"
    )
    assert isinstance(sim, list)

    sim = await d_client.vset().vsim(
        "myset",
        input="elem_1",
        filter=".index > 28 and .elem_name in ['elem_12', 'elem_17', 'elem_special']",
        filter_ef=500,
    )
    assert len(sim) == 1
    assert isinstance(sim, list)


@skip_if_server_version_lt("7.9.0")
async def test_vsim_truth_no_thread_enabled(d_client):
    elements_count = 5000
    vector_dim = 50
    for i in range(1, elements_count + 1):
        float_array = [random.uniform(10 * i, 1000 * i) for x in range(vector_dim)]
        await d_client.vset().vadd("myset", float_array, f"elem_{i}")

    await d_client.vset().vadd("myset", [-22 for _ in range(vector_dim)], "elem_man_2")

    sim_without_truth = await d_client.vset().vsim(
        "myset", input="elem_man_2", with_scores=True
    )
    sim_truth = await d_client.vset().vsim(
        "myset", input="elem_man_2", with_scores=True, truth=True
    )

    assert len(sim_without_truth) == 10
    assert len(sim_truth) == 10

    assert isinstance(sim_without_truth, dict)
    assert isinstance(sim_truth, dict)

    results_scores = list(
        zip(
            [v for _, v in sim_truth.items()], [v for _, v in sim_without_truth.items()]
        )
    )

    found_better_match = False
    for score_with_truth, score_without_truth in results_scores:
        if score_with_truth < score_without_truth:
            assert False, (
                "Score with truth [{score_with_truth}] < score without truth [{score_without_truth}]"
            )
        elif score_with_truth > score_without_truth:
            found_better_match = True

    assert found_better_match

    sim_no_thread = await d_client.vset().vsim(
        "myset", input="elem_man_2", with_scores=True, no_thread=True
    )

    assert len(sim_no_thread) == 10
    assert isinstance(sim_no_thread, dict)


@skip_if_server_version_lt("7.9.0")
async def test_vdim(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9, 0.1, 0.2]
    await d_client.vset().vadd("myset", float_array, "elem1")

    dim = await d_client.vset().vdim("myset")
    assert dim == len(float_array)

    await d_client.vset().vadd("myset_reduced", float_array, "elem1", reduce_dim=4)
    reduced_dim = await d_client.vset().vdim("myset_reduced")
    assert reduced_dim == 4

    with pytest.raises(redis.ResponseError):
        await d_client.vset().vdim("myset_unexisting")


@skip_if_server_version_lt("7.9.0")
async def test_vcard(d_client):
    n = 20
    for i in range(n):
        float_array = [random.uniform(0, 10) for x in range(1, 8)]
        await d_client.vset().vadd("myset", float_array, f"elem{i}")

    card = await d_client.vset().vcard("myset")
    assert card == n

    with pytest.raises(redis.ResponseError):
        await d_client.vset().vdim("myset_unexisting")


@skip_if_server_version_lt("7.9.0")
async def test_vrem(d_client):
    n = 3
    for i in range(n):
        float_array = [random.uniform(0, 10) for x in range(1, 8)]
        await d_client.vset().vadd("myset", float_array, f"elem{i}")

    resp = await d_client.vset().vrem("myset", "elem2")
    assert resp == 1

    card = await d_client.vset().vcard("myset")
    assert card == n - 1

    resp = await d_client.vset().vrem("myset", "elem2")
    assert resp == 0

    card = await d_client.vset().vcard("myset")
    assert card == n - 1

    resp = await d_client.vset().vrem("myset_unexisting", "elem1")
    assert resp == 0


@skip_if_server_version_lt("7.9.0")
async def test_vemb_bin_quantization(d_client):
    e = [1, 4.32, 0.0, 0.05, -2.9]
    await d_client.vset().vadd(
        "myset",
        e,
        "elem",
        quantization=QuantizationOptions.BIN,
    )
    emb_no_quant = await d_client.vset().vemb("myset", "elem")
    assert emb_no_quant == [1, 1, -1, 1, -1]

    emb_no_quant_raw = await d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_no_quant_raw["quantization"] == "bin"
    assert isinstance(emb_no_quant_raw["raw"], bytes)
    assert isinstance(emb_no_quant_raw["l2"], float)
    assert "range" not in emb_no_quant_raw


@skip_if_server_version_lt("7.9.0")
async def test_vemb_q8_quantization(d_client):
    e = [1, 10.32, 0.0, 2.05, -12.5]
    await d_client.vset().vadd("myset", e, "elem", quantization=QuantizationOptions.Q8)

    emb_q8_quant = await d_client.vset().vemb("myset", "elem")
    assert _validate_quantization(e, emb_q8_quant, tolerance=0.1)

    emb_q8_quant_raw = await d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_q8_quant_raw["quantization"] == "int8"
    assert isinstance(emb_q8_quant_raw["raw"], bytes)
    assert isinstance(emb_q8_quant_raw["l2"], float)
    assert isinstance(emb_q8_quant_raw["range"], float)


@skip_if_server_version_lt("7.9.0")
async def test_vemb_no_quantization(d_client):
    e = [1, 10.32, 0.0, 2.05, -12.5]
    await d_client.vset().vadd(
        "myset", e, "elem", quantization=QuantizationOptions.NOQUANT
    )

    emb_no_quant = await d_client.vset().vemb("myset", "elem")
    assert _validate_quantization(e, emb_no_quant, tolerance=0.1)

    emb_no_quant_raw = await d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_no_quant_raw["quantization"] == "f32"
    assert isinstance(emb_no_quant_raw["raw"], bytes)
    assert isinstance(emb_no_quant_raw["l2"], float)
    assert "range" not in emb_no_quant_raw


@skip_if_server_version_lt("7.9.0")
async def test_vemb_default_quantization(d_client):
    e = [1, 5.32, 0.0, 0.25, -5]
    await d_client.vset().vadd("myset", vector=e, element="elem")

    emb_default_quant = await d_client.vset().vemb("myset", "elem")
    assert _validate_quantization(e, emb_default_quant, tolerance=0.1)

    emb_default_quant_raw = await d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_default_quant_raw["quantization"] == "int8"
    assert isinstance(emb_default_quant_raw["raw"], bytes)
    assert isinstance(emb_default_quant_raw["l2"], float)
    assert isinstance(emb_default_quant_raw["range"], float)


@skip_if_server_version_lt("7.9.0")
async def test_vemb_fp32_quantization(d_client):
    float_array_fp32 = [1, 4.32, 0.11]
    # Convert the list of floats to a byte array in fp32 format
    byte_array = _to_fp32_blob_array(float_array_fp32)
    await d_client.vset().vadd("myset", byte_array, "elem")

    emb_fp32_quant = await d_client.vset().vemb("myset", "elem")
    assert _validate_quantization(float_array_fp32, emb_fp32_quant, tolerance=0.1)

    emb_fp32_quant_raw = await d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_fp32_quant_raw["quantization"] == "int8"
    assert isinstance(emb_fp32_quant_raw["raw"], bytes)
    assert isinstance(emb_fp32_quant_raw["l2"], float)
    assert isinstance(emb_fp32_quant_raw["range"], float)


@skip_if_server_version_lt("7.9.0")
async def test_vemb_unexisting(d_client):
    emb_not_existing = await d_client.vset().vemb("not_existing", "elem")
    assert emb_not_existing is None

    e = [1, 5.32, 0.0, 0.25, -5]
    await d_client.vset().vadd("myset", vector=e, element="elem")
    emb_elem_not_existing = await d_client.vset().vemb("myset", "not_existing")
    assert emb_elem_not_existing is None


@skip_if_server_version_lt("7.9.0")
async def test_vlinks(d_client):
    elements_count = 100
    vector_dim = 800
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        await d_client.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=8,
        )

    element_links_all_layers = await d_client.vset().vlinks("myset", "elem1")
    assert len(element_links_all_layers) >= 1
    for neighbours_list_for_layer in element_links_all_layers:
        assert isinstance(neighbours_list_for_layer, list)
        for neighbour in neighbours_list_for_layer:
            assert isinstance(neighbour, str)

    elem_links_all_layers_with_scores = await d_client.vset().vlinks(
        "myset", "elem1", with_scores=True
    )
    assert len(elem_links_all_layers_with_scores) >= 1
    for neighbours_dict_for_layer in elem_links_all_layers_with_scores:
        assert isinstance(neighbours_dict_for_layer, dict)
        for neighbour_key, score_value in neighbours_dict_for_layer.items():
            assert isinstance(neighbour_key, str)
            assert isinstance(score_value, float)

    float_array = [0.75, 0.25, 0.5, 0.1, 0.9]
    await d_client.vset().vadd("myset_one_elem_only", float_array, "elem1")
    elem_no_neighbours_with_scores = await d_client.vset().vlinks(
        "myset_one_elem_only", "elem1", with_scores=True
    )
    assert len(elem_no_neighbours_with_scores) >= 1
    for neighbours_dict_for_layer in elem_no_neighbours_with_scores:
        assert isinstance(neighbours_dict_for_layer, dict)
        assert len(neighbours_dict_for_layer) == 0

    elem_no_neighbours_no_scores = await d_client.vset().vlinks(
        "myset_one_elem_only", "elem1"
    )
    assert len(elem_no_neighbours_no_scores) >= 1
    for neighbours_list_for_layer in elem_no_neighbours_no_scores:
        assert isinstance(neighbours_list_for_layer, list)
        assert len(neighbours_list_for_layer) == 0

    unexisting_element_links = await d_client.vset().vlinks("myset", "unexisting_elem")
    assert unexisting_element_links is None

    unexisting_vset_links = await d_client.vset().vlinks("myset_unexisting", "elem1")
    assert unexisting_vset_links is None

    unexisting_element_links = await d_client.vset().vlinks(
        "myset", "unexisting_elem", with_scores=True
    )
    assert unexisting_element_links is None

    unexisting_vset_links = await d_client.vset().vlinks(
        "myset_unexisting", "elem1", with_scores=True
    )
    assert unexisting_vset_links is None


@skip_if_server_version_lt("7.9.0")
async def test_vinfo(d_client):
    elements_count = 100
    vector_dim = 800
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        await d_client.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=8,
            quantization=QuantizationOptions.BIN,
        )

    vset_info = await d_client.vset().vinfo("myset")
    assert vset_info["quant-type"] == "bin"
    assert vset_info["vector-dim"] == vector_dim
    assert vset_info["size"] == elements_count
    assert vset_info["max-level"] > 0
    assert vset_info["hnsw-max-node-uid"] == elements_count

    unexisting_vset_info = await d_client.vset().vinfo("myset_unexisting")
    assert unexisting_vset_info is None


@skip_if_server_version_lt("7.9.0")
async def test_vset_vget_attributes(d_client):
    float_array = [1, 4.32, 0.11]
    attributes = {"key1": "value1", "key2": "value2"}

    # validate vgetattrs when no attributes are set with vadd
    resp = await d_client.vset().vadd("myset", float_array, "elem1")
    assert resp == 1

    attrs = await d_client.vset().vgetattr("myset", "elem1")
    assert attrs is None

    # validate vgetattrs when attributes are set with vadd
    resp = await d_client.vset().vadd(
        "myset_with_attrs", float_array, "elem1", attributes=attributes
    )
    assert resp == 1

    attrs = await d_client.vset().vgetattr("myset_with_attrs", "elem1")
    assert attrs == attributes

    # Set attributes and get attributes
    resp = await d_client.vset().vsetattr("myset", "elem1", attributes)
    assert resp == 1
    attr_saved = await d_client.vset().vgetattr("myset", "elem1")
    assert attr_saved == attributes

    # Set attributes to None
    resp = await d_client.vset().vsetattr("myset", "elem1", None)
    assert resp == 1
    attr_saved = await d_client.vset().vgetattr("myset", "elem1")
    assert attr_saved is None

    # Set attributes to empty dict
    resp = await d_client.vset().vsetattr("myset", "elem1", {})
    assert resp == 1
    attr_saved = await d_client.vset().vgetattr("myset", "elem1")
    assert attr_saved is None

    # Set attributes provided as string
    resp = await d_client.vset().vsetattr("myset", "elem1", json.dumps(attributes))
    assert resp == 1
    attr_saved = await d_client.vset().vgetattr("myset", "elem1")
    assert attr_saved == attributes

    # Set attributes to unexisting element
    resp = await d_client.vset().vsetattr("myset", "elem2", attributes)
    assert resp == 0
    attr_saved = await d_client.vset().vgetattr("myset", "elem2")
    assert attr_saved is None

    # Set attributes to unexisting vset
    resp = await d_client.vset().vsetattr("myset_unexisting", "elem1", attributes)
    assert resp == 0
    attr_saved = await d_client.vset().vgetattr("myset_unexisting", "elem1")
    assert attr_saved is None


@skip_if_server_version_lt("7.9.0")
async def test_vrandmember(d_client):
    elements = ["elem1", "elem2", "elem3"]
    for elem in elements:
        float_array = [random.uniform(0, 10) for x in range(1, 8)]
        await d_client.vset().vadd("myset", float_array, element=elem)

    random_member = await d_client.vset().vrandmember("myset")
    assert random_member in elements

    members_list = await d_client.vset().vrandmember("myset", count=2)
    assert len(members_list) == 2
    assert all(member in elements for member in members_list)

    # Test with count greater than the number of elements
    members_list = await d_client.vset().vrandmember("myset", count=10)
    assert len(members_list) == len(elements)
    assert all(member in elements for member in members_list)

    # Test with negative count
    members_list = await d_client.vset().vrandmember("myset", count=-2)
    assert len(members_list) == 2
    assert all(member in elements for member in members_list)

    # Test with count equal to the number of elements
    members_list = await d_client.vset().vrandmember("myset", count=len(elements))
    assert len(members_list) == len(elements)
    assert all(member in elements for member in members_list)

    # Test with count equal to 0
    members_list = await d_client.vset().vrandmember("myset", count=0)
    assert members_list == []

    # Test with count equal to 1
    members_list = await d_client.vset().vrandmember("myset", count=1)
    assert len(members_list) == 1
    assert members_list[0] in elements

    # Test with count equal to -1
    members_list = await d_client.vset().vrandmember("myset", count=-1)
    assert len(members_list) == 1
    assert members_list[0] in elements

    # Test with unexisting vset & without count
    members_list = await d_client.vset().vrandmember("myset_unexisting")
    assert members_list is None

    # Test with unexisting vset & count
    members_list = await d_client.vset().vrandmember("myset_unexisting", count=5)
    assert members_list == []


@skip_if_server_version_lt("7.9.0")
async def test_vset_commands_without_decoding_responces(client):
    # test vadd
    elements = ["elem1", "elem2", "elem3"]
    for elem in elements:
        float_array = [random.uniform(0.5, 10) for x in range(0, 8)]
        resp = await client.vset().vadd("myset", float_array, element=elem)
        assert resp == 1

    # test vemb
    emb = await client.vset().vemb("myset", "elem1")
    assert len(emb) == 8
    assert isinstance(emb, list)
    assert all(isinstance(x, float) for x in emb)

    emb_raw = await client.vset().vemb("myset", "elem1", raw=True)
    assert emb_raw["quantization"] == b"int8"
    assert isinstance(emb_raw["raw"], bytes)
    assert isinstance(emb_raw["l2"], float)
    assert isinstance(emb_raw["range"], float)

    # test vsim
    vsim = await client.vset().vsim("myset", input="elem1")
    assert len(vsim) == 3
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], bytes)

    # test vsim with scores
    vsim_with_scores = await client.vset().vsim(
        "myset", input="elem1", with_scores=True
    )
    assert len(vsim_with_scores) == 3
    assert isinstance(vsim_with_scores, dict)
    assert isinstance(vsim_with_scores[b"elem1"], float)

    # test vlinks - no scores
    element_links_all_layers = await client.vset().vlinks("myset", "elem1")
    assert len(element_links_all_layers) >= 1
    for neighbours_list_for_layer in element_links_all_layers:
        assert isinstance(neighbours_list_for_layer, list)
        for neighbour in neighbours_list_for_layer:
            assert isinstance(neighbour, bytes)
    # test vlinks with scores
    elem_links_all_layers_with_scores = await client.vset().vlinks(
        "myset", "elem1", with_scores=True
    )
    assert len(elem_links_all_layers_with_scores) >= 1
    for neighbours_dict_for_layer in elem_links_all_layers_with_scores:
        assert isinstance(neighbours_dict_for_layer, dict)
        for neighbour_key, score_value in neighbours_dict_for_layer.items():
            assert isinstance(neighbour_key, bytes)
            assert isinstance(score_value, float)

    # test vinfo
    vset_info = await client.vset().vinfo("myset")
    assert vset_info[b"quant-type"] == b"int8"
    assert vset_info[b"vector-dim"] == 8
    assert vset_info[b"size"] == len(elements)
    assert vset_info[b"max-level"] >= 0
    assert vset_info[b"hnsw-max-node-uid"] == len(elements)

    # test vgetattr
    attributes = {"key1": "value1", "key2": "value2"}
    await client.vset().vsetattr("myset", "elem1", attributes)
    attrs = await client.vset().vgetattr("myset", "elem1")
    assert attrs == attributes

    # test vrandmember
    random_member = await client.vset().vrandmember("myset")
    assert isinstance(random_member, bytes)
    assert random_member.decode("utf-8") in elements

    members_list = await client.vset().vrandmember("myset", count=2)
    assert len(members_list) == 2
    assert all(member.decode("utf-8") in elements for member in members_list)


def _to_fp32_blob_array(float_array):
    """
    Convert a list of floats to a byte array in fp32 format.
    """
    # Convert the list of floats to a NumPy array with dtype np.float32
    arr = np.array(float_array, dtype=np.float32)
    # Convert the NumPy array to a byte array
    byte_array = arr.tobytes()
    return byte_array


def _validate_quantization(original, quantized, tolerance=0.1):
    original = np.array(original, dtype=np.float32)
    quantized = np.array(quantized, dtype=np.float32)

    max_diff = np.max(np.abs(original - quantized))
    if max_diff > tolerance:
        return False
    else:
        return True
